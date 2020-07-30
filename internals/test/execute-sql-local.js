const AWS = require('aws-sdk');
const stream = require('stream');
const { promisify } = require('util');
const fs = require('fs-extra');
const Path = require('path');
const got = require('got');
const tar = require('tar');
const zlib = require('zlib');
const AdmZip = require('adm-zip');
const { execFileSync } = require('child_process');

const pipeline = promisify(stream.pipeline);

const SPARK_DOWNLOAD_URL =
  'https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-1.0/spark-2.4.3-bin-hadoop2.8.tgz';

const GLUE_LIBS_DOWNLOAD_URL =
  'https://github.com/awslabs/aws-glue-libs/archive/glue-1.0.zip';

const SPARK_HOME = Path.normalize(`./.spark`);
const GLUE_LIBS = Path.normalize('./.glue-libs');
const GLUE_PY_SPARK = Path.join(GLUE_LIBS, 'bin/gluepyspark');

async function ensureSparkHome() {
  if (!fs.existsSync(SPARK_HOME)) {
    const sparkTarball = Path.join(SPARK_HOME, 'spark.tgz');

    await fs.ensureDir(SPARK_HOME);

    await pipeline(
      got.stream(SPARK_DOWNLOAD_URL),
      fs.createWriteStream(sparkTarball),
    );

    await tar.x(
      // or tar.extract(
      {
        file: sparkTarball,
        cwd: SPARK_HOME,
        strip: 1,
      },
    );
    await fs.remove(sparkTarball);
  }
}

async function ensureGlueLibs() {
  if (!fs.existsSync(GLUE_LIBS)) {
    const glueZip = Path.join(GLUE_LIBS, 'glue-libs.zip');

    await fs.ensureDir(GLUE_LIBS);

    await pipeline(
      got.stream(GLUE_LIBS_DOWNLOAD_URL),
      fs.createWriteStream(glueZip),
    );

    const zip = new AdmZip(glueZip);

    zip.getEntries().forEach(zipEntry => {
      if (zipEntry.isDirectory) {
        return;
      }
      // strip away the zip prefix
      const entryNameStripped = zipEntry.entryName.replace(
        /^[^\/\\]+[\/\\]/,
        '',
      );
      if (entryNameStripped.length !== 0) {
        zip.extractEntryTo(
          zipEntry,
          Path.join(GLUE_LIBS, Path.dirname(entryNameStripped)),
          /*maintainEntryPath*/ false,
          /*overwrite*/ true,
        );
      }
    });

    await fs.remove(glueZip);
  }
}

async function getCredentials() {
  const sts = new AWS.STS();
  const result = await sts
    .assumeRole({
      RoleArn: 'arn:aws:iam::102184641170:role/dev-iris',
      RoleSessionName: 'iris-dev-endpoint-testing',
    })
    .promise();
  const credentials = result.Credentials;

  return new AWS.Credentials(
    credentials.AccessKeyId,
    credentials.SecretAccessKey,
    credentials.SessionToken,
  );
}

async function main() {
  await ensureSparkHome();
  await ensureGlueLibs();
  const credentials = await getCredentials();
  await fs.chmod(GLUE_PY_SPARK, 0o777);

  execFileSync(GLUE_PY_SPARK, [], {
    stdio: 'inherit',
    env: {
      ...process.env,
      AWS_DEFAULT_REGION: 'us-east-1',
      AWS_REGION: 'us-east-1',
      SPARK_HOME,
      AWS_ACCESS_KEY_ID: credentials.accessKeyId,
      AWS_SECRET_ACCESS_KEY: credentials.secretAccessKey,
      AWS_SESSION_TOKEN: credentials.sessionToken,
      PYSPARK_PYTHON: 'python3',
    },
  });
}

main().catch(e => {
  console.error(e.message);
});
