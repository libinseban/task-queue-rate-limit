const cluster = require('cluster');
const os = require('os');
const express = require('express');
const fs = require('fs');
const redis = require('redis');
const { RateLimiterMemory } = require('rate-limiter-flexible');

const redisClient = redis.createClient();
const PORT = process.env.PORT || 3000;

if (cluster.isMaster) {
  const numCPUs = os.cpus().length;
  console.log(`Master process running with PID ${process.pid}`);

  for (let i = 0; i < 2; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died`);
    cluster.fork();
  });
} else {
  const app = express();
  app.use(express.json());

  const taskQueue = {};
  const rateLimiter = new RateLimiterMemory({
    points: 20, 
    duration: 60,
    blockDuration: 0
  });

  const taskLimiterPerSec = new RateLimiterMemory({
    points: 1, 
    duration: 1
  });

  const task = async (user_id) => {
    const logData = `${user_id} - task completed at - ${new Date().toISOString()}\n`;
    fs.appendFile('task-log.txt', logData, (err) => {
      if (err) {
        console.error('Failed to write to log file', err);
      }
    });
    console.log(logData);
  };

  const processTask = async (user_id) => {
    await task(user_id);

    if (taskQueue[user_id] && taskQueue[user_id].length > 0) {
      setTimeout(() => {
        const nextTask = taskQueue[user_id].shift();
        processTask(nextTask);
      }, 1000); 
    }
  };

  app.post('/task', async (req, res) => {
    const { user_id } = req.body;

    if (!user_id) {
      return res.status(400).json({ error: 'User ID is required' });
    }

    try {
      await taskLimiterPerSec.consume(user_id);
      await rateLimiter.consume(user_id);

      processTask(user_id);
      res.status(200).json({ message: `Task for ${user_id} is being processed` });
    } catch (rateLimiterRes) {
      if (!taskQueue[user_id]) {
        taskQueue[user_id] = [];
      }
      taskQueue[user_id].push(user_id);

      res.status(429).json({
        message: `Task for ${user_id} is rate limited and queued`
      });
    }
  });

  app.listen(PORT, () => {
    console.log(`Worker ${process.pid} is listening on port ${PORT}`);
  });
}
