# encoding: utf-8

import os
import sys
import ujson
import asyncio

import random

from datetime import datetime
from collections import defaultdict

import aiohttp
import aiofiles

from websockets.exceptions import ConnectionClosed

from sanic import Sanic, response
from sanic.exceptions import NotFound

from playhouse.shortcuts import model_to_dict, dict_to_model

from models import Repo, Job, db, Worker

app = Sanic()

APPS_LISTS = {
    "Official": "https://app.yunohost.org/official.json",
    "Community": "https://app.yunohost.org/community.json",
}

subscriptions = defaultdict(list)

# this will have the form:
# jobs_in_memory_state = {
#     some_job_id: {"worker": some_worker_id, "task": some_aio_task},
# }
jobs_in_memory_state = {}


def reset_pending_jobs():
    Job.update(state="scheduled").where(Job.state == "running").execute()


def reset_busy_workers():
    # XXX when we'll have distant workers that might break those
    Worker.update(state="available").execute()


async def monitor_apps_lists():
    "parse apps lists every hour or so to detect new apps"

    # only support github for now :(
    async def get_master_commit_sha(app_id, organization="yunohost-apps"):
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://api.github.com/repos/{organization}/{app_id}_ynh/branches/master") as response:
                data = await response.json()
                try:
                    commit_sha = data["commit"]["sha"]
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    print(f"Error response: {data}")
                    raise

                return commit_sha

    for app_list_name, url in APPS_LISTS.items():
        async with aiohttp.ClientSession() as session:
            app_list = "official"
            sys.stdout.write(f"Downloading {app_list_name}.json...\r")
            sys.stdout.flush()
            async with session.get(url) as resp:
                data = await resp.json()
                sys.stdout.write(f"Downloading {app_list_name}.json...done\n")

        repos = {x.name: x for x in Repo.select().where(Repo.app_list == app_list_name)}

        for app_id, app_data in data.items():
            commit_sha = await get_master_commit_sha(app_id)
            print(f"{app_id} → {commit_sha}")

            # already know, look to see if there is new commits
            if app_id in repos:
                repo = repos[app_id]
                if repo.revision != commit_sha:
                    print(f"Application {app_id} has new commits on github, schedule new job")
                    repo.revision = commit_sha
                    repo.save()

                    job = Job.create(
                        name=f"{app_id} ({app_list_name})",
                        url_or_path=repo.url,
                        target_revision=commit_sha,
                        yunohost_version="stretch-stable",
                        state="scheduled",
                    )

                    await broadcast({
                        "action": "new_job",
                        "data": model_to_dict(job),
                    }, "jobs")

            # new app
            else:
                print(f"New application detected: {app_id} in {app_list_name}")
                repo = Repo.create(
                    name=app_id,
                    url=app_data["git"]["url"],
                    revision=commit_sha,
                    app_list=app_list_name,
                )

                print(f"Schedule a new build for {app_id}")
                job = Job.create(
                    name=f"{app_id} ({app_list_name})",
                    url_or_path=repo.url,
                    target_revision=commit_sha,
                    yunohost_version="stretch-stable",
                    state="scheduled",
                )

                await broadcast({
                    "action": "new_job",
                    "data": model_to_dict(job),
                }, "jobs")

            await asyncio.sleep(3)

    await asyncio.sleep(5 * 60)
    asyncio.ensure_future(monitor_apps_lists())


async def jobs_dispatcher():
    if Worker.select().count() == 0:
        for i in range(1):
            Worker.create(state="available")

    while True:
        workers = Worker.select().where(Worker.state == "available")

        # no available workers, wait
        if workers.count() == 0:
            await asyncio.sleep(3)
            continue

        with db.atomic('IMMEDIATE'):
            jobs = Job.select().where(Job.state == "scheduled")

            # no jobs to process, wait
            if jobs.count() == 0:
                await asyncio.sleep(3)
                continue

            for i in range(min(workers.count(), jobs.count())):
                job = jobs[i]
                worker = workers[i]

                job.state = "running"
                job.started_time = datetime.now()
                job.save()

                worker.state = "busy"
                worker.save()

                jobs_in_memory_state[job.id] = {
                    "worker": worker.id,
                    "task": asyncio.ensure_future(run_job(worker, job)),
                }


async def run_job(worker, job):
    await broadcast({
        "action": "update_job",
        "data": model_to_dict(job),
    }, ["jobs", f"job-{job.id}"])

    # fake stupid command, whould run CI instead
    print(f"Starting job {job.name}...")

    cwd = os.path.split(path_to_analyseCI)[0]
    arguments = f' {job.url_or_path} "{job.name}"'
    print("/bin/bash " + path_to_analyseCI + arguments)
    command = await asyncio.create_subprocess_shell("/bin/bash " + path_to_analyseCI + arguments,
                                                    cwd=cwd,
                                                    stdout=asyncio.subprocess.PIPE,
                                                    stderr=asyncio.subprocess.PIPE)

    while not command.stdout.at_eof():
        data = await command.stdout.readline()
        line = data.decode().rstrip()

        job.log += data.decode()
        # XXX seems to be okay performance wise but that's probably going to be
        # a bottleneck at some point :/
        # theoritically jobs are going to have slow output
        job.save()

        await broadcast({
            "action": "update_job",
            "id": job.id,
            "data": model_to_dict(job),
        }, ["jobs", f"job-{job.id}"])

    # XXX stupid crap to stimulate long jobs
    await asyncio.sleep(random.randint(1, 15))
    # await asyncio.sleep(5)
    print(f"Finished job {job.name}")

    await command.wait()
    job.end_time = datetime.now()
    job.state = "done"
    job.save()

    # remove ourself from the state
    del jobs_in_memory_state[job.id]

    worker.state = "available"
    worker.save()

    await broadcast({
        "action": "update_job",
        "id": job.id,
        "data": model_to_dict(job),
    }, ["jobs", f"job-{job.id}"])


async def broadcast(message, channels):
    if not isinstance(channels, (list, tuple)):
        channels = [channels]

    for channel in channels:
        ws_list = subscriptions[channel]
        dead_ws = []

        for ws in ws_list:
            try:
                await ws.send(ujson.dumps(message))
            except ConnectionClosed:
                dead_ws.append(ws)

    for to_remove in dead_ws:
        ws_list.remove(to_remove)


def subscribe(ws, channel):
    subscriptions[channel].append(ws)


@app.websocket('/index-ws')
async def index_ws(request, websocket):
    subscribe(websocket, "jobs")

    await websocket.send(ujson.dumps({
        "action": "init_jobs",
        "data": map(model_to_dict, Job.select().order_by(-Job.id)),
    }))

    while True:
        data = await websocket.recv()
        print(f"websocket: {data}")
        await websocket.send(f"echo {data}")


@app.websocket('/job-<job_id>-ws')
async def job_ws(request, websocket, job_id):
    job = Job.select().where(Job.id == job_id)

    if job.count == 0:
        raise NotFound()

    job = job[0]

    subscribe(websocket, f"job-{job.id}")

    await websocket.send(ujson.dumps({
        "action": "init_job",
        "data": model_to_dict(job),
    }))

    while True:
        data = await websocket.recv()
        print(f"websocket: {data}")
        await websocket.send(f"echo {data}")


@app.route("/api/job", methods=['POST'])
async def api_new_job(request):
    # TODO auth or some kind

    # test type ?
    # architecture (ARM)
    # debian version
    # yunohost version (in test type?
    # day for the montly test?
    # ? analyseCI path ?

    job = Job.create(
        name=request.json["name"],
        url_or_path=request.json["url_or_path"],
        target_revision=request.json["revision"],
        type=request.json.get("test_type", "stable"),
        yunohost_version=request.json.get("yunohost_version", "unstable"),
        debian_version=request.json.get("debian_version", "stretch"),
    )

    print(f"Request to add new job '{job.name}' {job}")

    await broadcast({
        "action": "new_job",
        "data": model_to_dict(job),
    }, "jobs")

    return response.text("ok")


@app.route("/api/job/<job_id>/stop", methods=['POST'])
async def api_stop_job(request, job_id):
    # TODO auth or some kind

    job = Job.select().where(Job.id == job_id)

    if job.count == 0:
        raise NotFound()

    job = job[0]

    if job.state == "scheduled":
        job.state = "canceled"
        job.save()

        await broadcast({
            "action": "update_job",
            "data": model_to_dict(job),
        }, ["jobs", f"job-{job.id}"])

        return response.text("ok")

    if job.state == "running":
        job.state = "canceled"
        job.save()

        jobs_in_memory_state[job.id]["task"].cancel()

        worker = Worker.select().where(Worker.id == jobs_in_memory_state[job.id]["worker"])[0]
        worker.state = "available"
        worker.save()

        await broadcast({
            "action": "update_job",
            "data": model_to_dict(job),
        }, ["jobs", f"job-{job.id}"])

        return response.text("ok")

    if job.state in ("done", "canceled", "failure"):
        # nothing to do, task is already done
        return response.text("ok")

    raise Exception(f"Tryed to cancel a job with an unknown state: {job.state}")


@app.route('/job/<job_id>')
async def job(request, job_id):
    job = Job.select().where(Job.id == job_id)

    if job.count == 0:
        raise NotFound()

    job = job[0]

    async with aiofiles.open("./templates/job.html", mode="r") as index_template:
        return response.html(await index_template.read() % job.id)


@app.route('/')
async def index(request):
    async with aiofiles.open("./templates/index.html", mode="r") as index_template:
        return response.html(await index_template.read())


if __name__ == "__main__":
    if not sys.argv[1:]:
        print("Error: missing shell path argument")
        print("Usage: python run.py /path/to/analyseCI.sh")
        sys.exit(1)

    path_to_analyseCI = sys.argv[1]

    reset_pending_jobs()
    reset_busy_workers()

    app.add_task(monitor_apps_lists())
    app.add_task(jobs_dispatcher())
    app.run('localhost', port=4242, debug=True)
