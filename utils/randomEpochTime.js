function getRandomEpochTime() {
    let currentEpochTime = Math.floor(Date.now() / 1000);
    let randomTime = Math.floor(Math.random() * 1e6);
    return currentEpochTime - randomTime;
}

module.exports = { getRandomEpochTime } 