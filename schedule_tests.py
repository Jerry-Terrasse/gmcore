#!/usr/bin/env python3

import argparse
import os
import subprocess
import time
import signal
from multiprocessing import Queue, Process
from loguru import logger
logger.add('schedule_tests.log', level='DEBUG')

from typing import Optional

class Task:
	def __init__(self, name: str, force_np: int = -1):
		self.name = name # 'stop' for end of task-queue
		if 'swm' in name:
			self.exe = 'gmcore_swm_driver.exe'
		elif 'adv' in name:
			self.exe = 'gmcore_adv_driver.exe'
		else:
			self.exe = 'gmcore_driver.exe'
		self.force_np = force_np # TODO

@logger.catch(reraise=True)
def worker(queue: Queue, done_q: Queue, first_tasks: list[Task], root: str, work_dir: str, name: str, node: str, np: int):
	work_dir_abs = os.path.join(root, work_dir, 'GMCORE-TESTBED')

	times: dict[str, float] = {}
	while True:
		if first_tasks:
			from_q = False
			task = first_tasks.pop()
		else:
			from_q = True
			task = queue.get()
		if task.name == 'stop':
			break
		logger.info(f'{name} got task {task.name}')
		task_np = task.force_np if task.force_np > 0 else np

		start_time = time.time()
		mpiexec(os.path.join(root, 'build', task.exe), os.path.join(work_dir_abs, task.name), node, task_np)
		end_time = time.time()

		if not from_q and first_tasks == []: # just finished all first-tasks
			done_q.put(name)
		logger.debug(f'{name} finished task {task.name} in {end_time - start_time} seconds')
		times[task.name] = end_time - start_time
	tot_time = sum(times.values())
	logger.info(f'{name} exits. Total time: {tot_time} seconds')

def run(cmd):
	logger.debug(f'==> {cmd}')
	res = subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE)
	assert res.returncode == 0, f'Error: {cmd} failed!'

def clean_job(sig, frame):
	run(f'scancel -n {job_name}')
	exit(1)

def mpiexec(exe: str, exec_dir: str, host: str, np: int, namelist: str = 'namelist'):
	# return time.sleep(np / 10 + int(host[-1])*10)
	os.chdir(exec_dir)
	run(f'nice -n -20 mpiexec -hosts {host} -np {np} {exe} {namelist}')

@logger.catch()
def main():
	parser = argparse.ArgumentParser('Run GMCORE tests.')
	# parser.add_argument('--slurm', help='Use SLURM job manager', action='store_true')
	# parser.add_argument('-q', '--queue', help='Job queue')
	# parser.add_argument('-n', '--np', help='Processes to use for running tests', type=int, default=2)
	# parser.add_argument('-p', '--ntasks-per-node', type=int, default=20)
	# parser.add_argument('-m', '--node-list')
	# parser.add_argument('-x', '--exclude-nodes', nargs='+', default=[])
	parser.add_argument('-w', '--work-root', help='Where to run tests', required=True)
	parser.add_argument('-c', '--cases', help='Which cases to run', nargs='+', default=[])
	# parser.add_argument('--workers', type=) # TODO
	args = parser.parse_args()
	logger.info(f'Arguments: {args}')

	gmcore_root = os.path.dirname(os.path.realpath(__file__))
	assert os.path.isdir(args.work_root), f'Error: {args.work_root} does not exist!'

	if not os.path.isdir(os.path.join(args.work_root, 'GMCORE-TESTBED')):
		logger.info('Cloning testbed...')
		run('git clone https://gitee.com/dongli85/GMCORE-TESTBED')
	
	if args.cases == []:
		args.cases = [
			'adv_sr.360x180', 'adv_mv.360x180', 'adv_dc4.360x180', 'adv_dcmip12.360x180',
			'swm_rh.180x90', 'swm_rh.360x180', 'swm_mz.180x90', 'swm_mz.360x180', 'swm_jz.180x90', 'swm_jz.360x180',
			'rh.180x90', 'mz.180x90', 'bw.180x90', 'bw.360x180',
			# 'rh.360x180', 'mz.360x180',
		]#[::-1]
	logger.info(f'Cases to run: {args.cases}')

	task_queue = Queue()
	done_queue = Queue()

	workers_info = [
		{'name': 'worker_1_1', 'node': 'node1', 'np': 30},
		{'name': 'worker_1_2', 'node': 'node1', 'np': 30},
		{'name': 'worker_2_1', 'node': 'node2', 'np': 30},
		{'name': 'worker_2_2', 'node': 'node2', 'np': 30}
	]

	first_tasks = [
		[Task('rh.360x180', 60)],
		[],
  		[Task('mz.360x180', 60)],
		[],
	]
	workers: list[Process] = []
	logger.info('Launching workers:')
	for idx, info in enumerate(workers_info):
		logger.info(f"{idx}: {info['name']} on {info['node']} with {info['np']} processes")
		proc = Process(target=worker, args=(task_queue, done_queue, first_tasks[idx], gmcore_root, args.work_root), kwargs=info)
		# proc.start()
		workers.append(proc)

	start_time = time.time()
	logger.info(f'Start running at {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))}')

	# Manual Tasks
	# task_queue.put(Task('rh.360x180', 60))
	# task_queue.put(Task('mz.360x180', 60))
	workers[0].start()
	workers[2].start()

	for case in args.cases:
		task_queue.put(Task(case))

	for _ in range(2):
		done_name = done_queue.get()
		logger.info(f'{done_name} has finished its first-tasks.')
		if done_name == 'worker_1_1':
			workers[1].start()
		elif done_name == 'worker_2_1':
			workers[3].start()
		else:
			raise ValueError(f'Error: Unexpected worker name {done_name}')

	# Auto Tasks
	for proc in workers:
		if not proc.is_alive():
			proc.start()

	for _ in workers:
		task_queue.put(Task('stop'))

	for proc in workers:
		proc.join()
		assert proc.exitcode == 0, f'Error: {proc.name} exited with code {proc.exitcode}'
	logger.success('All workers joined.')

	end_time = time.time()
	logger.info(f'Finished at {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))}')
	logger.success(f'Total time: {end_time - start_time} seconds, {(end_time - start_time) / 60 / 60} hours')

if __name__ == '__main__':
	job_name = 'gmcore_' + str(int(time.time()))
	signal.signal(signal.SIGINT, clean_job)
	main()