const Redis = require("ioredis");
const redis = new Redis({
  port: process.env.REDIS_PORT,
  host: process.env.REDIS_IP,
  connectTimeout: 10000,
});

redis.on("connect", () => {
  console.log("Redis Connected Successfully");
});

const createRedisData = async (req, res) => {
  const firstname = req.body.firstname;
  const lastname = req.body.lastname;
  const email = req.body.email;
  const password = req.body.password;

  const response = await redis.hset("userRahul", {
    firstname: firstname,
    lastname: lastname,
    email: email,
    password: password,
  });
  res.json({
    response,
    message: "User saved successfully",
  });
};

const getRedisData = async (req, res) => {
  const response = await redis.hgetall("userRahul");
  res.json({
    response,
  });
};

const deleteRedisData = async (req, res) => {
  console.log(req.params.name);
  const result = await redis.del(req.params.name);
  console.log(result);
  res.json({ result });
};

module.exports = {
    createRedisData,
    getRedisData,
    deleteRedisData
}