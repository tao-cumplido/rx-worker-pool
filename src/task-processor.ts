import { parentPort } from "node:worker_threads";

import type { Observer } from "rxjs";
import { Subject } from "rxjs";

export function taskProcessor<Task, Value>(process: (task: Task, observer: Observer<Value>) => void | Promise<void>) {
	if (!parentPort) {
		throw new Error("taskProcessor has to be called from a worker thread");
	}

	parentPort.on("message", async (task?: Task) => {
		if (typeof task === "undefined") {
			parentPort!.postMessage({ type: "init", });
			return;
		}

		const subject = new Subject<Value>();

		subject.subscribe({
			next: (value) => parentPort!.postMessage({ type: "next", value, }),
			complete: () => parentPort!.postMessage({ type: "complete", }),
		});

		process(task, subject);
	});
}
