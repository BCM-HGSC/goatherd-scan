from multiprocessing import Process, Queue
from os import getpid
from queue import Empty
from sys import argv
from time import sleep

from local_file_metadata import LocalFileMetadata, scan


def main() -> None:
    output_path, *extra = argv
    start_path = "."
    if extra:
        assert len(extra) == 1, extra
        start_path, = extra
    run(start_path, output_path)

    
def run(start_path, output_path) -> None:
    print(getpid())
    engine = TaskEngine(run_worker, handle_results, 2)
    for i in range(10):
        engine.post_task(i)
    sleep(1)
    engine.start()
    for i in range(10, 20):
        engine.post_task(i)
    engine.finish()
    print("done")



def run_worker(worker_id, tasks: Queue, results: Queue) -> None:
    pid = getpid()
    print(pid, worker_id, "hello")
    while True:
        # print(pid, worker_id, "get")
        task_def = tasks.get()
        print(*task_def, pid, worker_id, "value")
        results.put((task_def + (pid, worker_id)))
        # print(pid, worker_id, "sleep")
        sleep(0.1)
        # print(pid, i, "wake")


WORK_END = "WORK END"  # sentinel object for task_defs Queue


def handle_results(worker_results: Queue, task_defs: Queue):
    print("handle_results")
    remaining_tasks = set()
    task_defs_closed = False
    while remaining_tasks or not task_defs_closed:
        if not task_defs_closed:
            for task_def in iterate_quick(task_defs):
                if not task_def == WORK_END:
                    assert isinstance(task_def, tuple), task_def
                    task_id = task_def[0]
                    remaining_tasks.add(task_id)
                else:
                    print("Got WORK_END")
                    task_defs_closed = True
        print("fetching results")
        for result in iterate_quick(worker_results):
            print("result", *result)
            task_id = result[0]
            assert task_id in remaining_tasks, (result, remaining_tasks)
            remaining_tasks.remove(task_id)
    assert not remaining_tasks, remaining_tasks


def iterate_quick(q: Queue):
    while True:
        try:
            yield q.get(True, 0.01)
        except Empty:
            return


class TaskEngine:
    def __init__(self, worker_function, results_handler, num_workers: int = 2):
        self.worker_tasks = Queue()
        self.worker_results = Queue()
        self.task_defs = Queue()
        self.workers = [
            Process(
                target=worker_function,
                args=(
                    worker_id,
                    self.worker_tasks,
                    self.worker_results,
                ),
            )
            for worker_id in range(num_workers)
        ]
        for worker in self.workers:
            worker.start()
        self.result_process = Process(
            target=results_handler, args=(self.worker_results, self.task_defs)
        )

    def post_task(self, *data):
        print("posting", *data)
        # remaining_tasks.add(data)
        self.worker_tasks.put(data)
        self.task_defs.put(data)

    def start(self):
        self.result_process.start()

    def finish(self):
        """Wait for self.result_process to exit, then kill all workers and exit."""
        self.task_defs.put(WORK_END)
        self.worker_tasks.close()
        self.task_defs.close()
        self.result_process.join()
        for worker in self.workers:
            print("terminating", worker.name)
            worker.terminate()


if __name__ == "__main__":
    main()
