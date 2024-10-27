import os from "node:os";
import { Worker, type WorkerOptions } from "node:worker_threads";

import { lastValueFrom, Subject, type Observable } from "rxjs";

export type WorkerMessage<T> = {
	readonly type: "init" | "complete";
} | {
	readonly type: "next";
	readonly value: T;
} | {
	readonly type: "error";
	readonly error: unknown;
};

export type PendingTask<Task, Value> = {
	readonly task: Task;
	readonly subject: Subject<Value>;
};

export type WorkerPoolOptions = {
	readonly size?: number | undefined;
	readonly workerOptions?: WorkerOptions | undefined;
};

export class WorkerPool<Task, Value> {
	readonly filename: URL;

	readonly #pool: Worker[];
	readonly #workers: readonly Worker[];
	readonly #pendingTasks: PendingTask<Task, Value>[];

	constructor(filename: URL, options?: WorkerPoolOptions) {
		this.filename = filename;
		this.#pool = Array.from({ length: options?.size ?? os.availableParallelism(), }).map(() => new Worker(filename, options?.workerOptions));
		this.#workers = Array.from(this.#pool);
		this.#pendingTasks = [];
	}

	addTask(task: Task): Observable<Value> {
		const pending = {
			task,
			subject: new Subject<Value>(),
		};

		this.#pendingTasks.push(pending);

		const worker = this.#pool.pop();

		if (worker) {
			this.#runNext(worker);
		}

		return pending.subject.asObservable();
	}

	async close(): Promise<void> {
		await Promise.all(this.#pendingTasks.map(({ subject, }) => lastValueFrom(subject, { defaultValue: null, })));
		await Promise.all(this.#workers.map((worker) => worker.terminate()));
	}

	async [Symbol.asyncDispose](): Promise<void> {
		await this.close();
	}

	#runNext(worker: Worker, pending = this.#pendingTasks.shift()) {
		if (pending) {
			worker.once("message", ({ type, }: WorkerMessage<Value>) => {
				if (type !== "init") {
					throw new Error(`invalid message type: expected 'init', got '${type}'`);
				}

				this.#observe(worker, pending);
			});

			worker.postMessage(undefined);
		}
	}

	#observe(worker: Worker, { task, subject, }: PendingTask<Task, Value>) {
		const unsubscribe = () => {
			worker.off("error", handleError);
			worker.off("messageerror", handleError);
			worker.off("exit", handleExit);
			worker.off("message", handleMessage);
		};

		const handleError = (error: unknown) => {
			subject.error(error);
			unsubscribe();
		};

		const handleExit = (code: number) => {
			subject.error(new Error(`unexpected worker exit with code ${code}`));
		};

		const handleMessage = (message: WorkerMessage<Value>) => {
			if (message.type === "error") {
				subject.error(message.error);
				return;
			}

			if (message.type === "next") {
				subject.next(message.value);
				return;
			}

			if (message.type === "complete") {
				unsubscribe();
				subject.complete();

				const next = this.#pendingTasks.shift();

				if (next) {
					this.#runNext(worker, next);
				} else {
					this.#pool.push(worker);
				}
			}
		};

		worker.on("error", handleError);
		worker.on("messageerror", handleError);
		worker.on("exit", handleExit);
		worker.on("message", handleMessage);

		worker.postMessage(task);
	}
}
