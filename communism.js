/**
 * communism.js - A Node library for Workers
 * @author Andrés Brugarolas Martínez
 * @license PROSL (Progressive Restricted Open Software License)
 * @see LICENSE
 * @version 1.0.0
 *
 * A simple library to execute async functions in separate workers.
 * Consist of two functions:
 * - comrade(asyncFunction): Executes the given async function in a separate thread.
 * - workersParty(asyncFunction, numThreads): Executes the given async function in a separate thread pool.
 */
const { Worker, isMainThread, parentPort, cpus } = require('worker_threads');
const Reactivefy = require('reactivefy/observables/full.js'); // npm install --save reactivefy

/**
 * Executes the given async function in a separate thread.
 * @author Andrés Brugarolas Marínez
 * @license PROSL
 *
 * Example:
  if (isMainThread) {
    const asyncFn = comrade(async (a, b) => {
      return a + b;
    });

    asyncFn(1, 2).then(console.log);  // Outputs: 3
  }
 *
 * @param {Function} asyncFunction
 * @returns Promise
 */
function comrade(asyncFunction) {
  let currentId = 0;
  const promises = {};

  if (isMainThread) {
    const worker = new Worker(__filename);  // assuming this script is the worker

    worker.on('message', (e) => {
      promises[e.data[0]][e.data[1]](e.data[2]);
      promises[e.data[0]] = null;
    });

    return function (...args) {
      return new Promise((resolve, reject) => {
        promises[++currentId] = [resolve, reject];
        worker.postMessage([currentId, args]);
      });
    };
  } else {
    parentPort.on('message', (e) => {
      Promise.resolve(e[1])
        .then(args => asyncFunction(...args))
        .then(
          result => parentPort.postMessage([e[0], 0, result]),
          error => parentPort.postMessage([e[0], 1, '' + error])
        );
    });
  }
}

/**
 * Executes the given async function in a separate thread pool.
 * @author Andrés Brugarolas Marínez
 * @license PROSL
 *
 * Example:
  if (isMainThread) {
    const { asyncFn } = workersParty(async (a, b) => {
      if (a === 3) throw new Error('Simulated error for a=3');
      return a + b;
    });

    const obj = asyncFn([[1, 2], [3, 4]]);
    const { tasks } = obj;
    console.log(tasks.done, tasks.failed, tasks.todo, tasks.total); // Outputs: 0 0 2 2

    obj.finishWork().promise.then(() => {
      console.log("All tasks are done!");
    });
  }
 *
 * @param {Function} asyncFunction
 * @param {Number} numThreads (optional) defaults to number of cpus - 1
 * @returns Promise
 */
function workersParty(asyncFunction, numThreads) {
  if (!numThreads) {
    numThreads = Math.max(1, Math.floor(cpus().length / 2) - 1);
  }

  if (isMainThread) {
    let tasksQueue = [];
    const reactiveTasks = Reactivefy.observe({
      done: 0,
      failed: 0,
      todo: 0,
      total: 0
    });
    let isFinished = false;
    let resolveAllDone;
    const allDonePromise = new Promise(resolve => {
      resolveAllDone = resolve;
    });

    const workers = Array.from({ length: numThreads }).map(() => {
      const worker = new Worker(__filename);  // assuming this script is the worker

      worker.on('message', (e) => {
        if (e === 'request') {
          nextTask(worker);
        } else if (e === 'done') {
          reactiveTasks.done++;
          if (reactiveTasks.done + reactiveTasks.failed === reactiveTasks.total && isFinished) {
            resolveAllDone();
          }
        } else if (e === 'error') {
          reactiveTasks.failed++;
        }
      });

      return worker;
    });

    function nextTask(worker) {
      if (tasksQueue.length > 0) {
        const taskArgs = tasksQueue.shift();
        worker.postMessage(['task', taskArgs]);
      } else if (isFinished) {
        worker.postMessage(['terminate']);
      } else {
        worker.postMessage(['wait']);
      }
    }

    function finishWork() {
      isFinished = true;
      workers.forEach(worker => nextTask(worker));
      return allDonePromise;
    }

    return {
      asyncFn: function (argsList) {
        if (isFinished) {
          console.error("Warning: No more tasks can be added after calling finishWork.");
          return;
        }
        tasksQueue = tasksQueue.concat(argsList);
        reactiveTasks.todo += argsList.length;
        reactiveTasks.total += argsList.length;

        workers.forEach(worker => nextTask(worker));  // dispatch tasks to available workers

        return {
          finishWork,
          tasks: reactiveTasks,
          promise: allDonePromise
        };
      }
    };
  } else {
    parentPort.on('message', (message) => {
      if (message[0] === 'task') {
        const args = message[1];
        asyncFunction(...args)
          .then(() => {
            parentPort.postMessage('done');
            parentPort.postMessage('request');
          })
          .catch(err => {
            console.error(err);
            parentPort.postMessage('error');
            parentPort.postMessage('request');
          });
      } else if (message[0] === 'terminate') {
        process.exit(0);
      } else if (message[0] === 'wait') {
        setTimeout(() => {
          parentPort.postMessage('request');
        }, 100);
      }
    });

    // Initially, workers will request for tasks
    parentPort.postMessage('request');
  }
}

module.exports = {
  comrade,
  workersParty
};
