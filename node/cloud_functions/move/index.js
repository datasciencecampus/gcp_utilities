const { Storage } = require("@google-cloud/storage");

const storage = new Storage();

const OBJECT_FINALIZE = "OBJECT_FINALIZE";

const missingEnvVar = (name) => {
  throw new Error(`Environment variable '${name}' not set`);
};

const requrieEnvVars = (list) =>
  list.map((item) => {
    if (!process.env[item]) {
      missingEnvVar(item);
    }
  });

async function moveBucketFile(bucketId, objectId) {
  const sourceBucket = storage.bucket(bucketId);
  const destBucket = storage.bucket(process.env.BUCKET_DESTINATION);

  await sourceBucket
    .file(objectId)
    .createReadStream()
    .pipe(destBucket.file(`${bucketId}/${objectId}`).createWriteStream());
}

const move = async (event) => {
  requrieEnvVars(["BUCKET_DESTINATION"]);

  const {
    attributes: { objectId, bucketId, eventType },
  } = event;
  console.log(JSON.stringify({ objectId, bucketId, eventType }));

  if (eventType !== OBJECT_FINALIZE) {
    console.log(`Event type is not ${OBJECT_FINALIZE} exiting without moving.`);
    return;
  }

  try {
    await moveBucketFile(bucketId, objectId);
  } catch (e) {
    console.log(e);
  }
};

module.exports = {
  move,
};
