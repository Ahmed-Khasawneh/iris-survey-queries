const AWS = require('aws-sdk');
const stream = require('stream');
const { promisify } = require('util');
const os = require('os');
const fs = require('fs-extra');
const Path = require('path');
const got = require('got');
const tar = require('tar');
const { spawn } = require('child_process');
const { argv } = require('yargs');
const AdmZip = require('adm-zip');
const uuid = require('uuid');
const ora = require('ora');
const moment = require('moment');
const SurveyTypes = require('./survey-types');
const SparkStageManager = require('./spark-stage-manager');

const pipeline = promisify(stream.pipeline);

const OUTPUT_PATH_PARSER = /##OUTPUT##: (s3:\/\/[^\s]+)/;
const S3_REGEX_PARSER = /s3:\/\/([^\/]+)\/?(.*)/;
const SPARK_DOWNLOAD_URL =
  'https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-1.0/spark-2.4.3-bin-hadoop2.8.tgz';

const GLUE_LIBS_DOWNLOAD_URL =
  'https://github.com/awslabs/aws-glue-libs/archive/glue-1.0.zip';

const SPARK_JOB = Path.normalize('./internals/pyspark/spark-executor.py');
const SPARK_HOME = Path.normalize(`./.spark`);
const GLUE_LIBS = Path.normalize('./.glue-libs');
// const GLUE_PY_SPARK = Path.join(GLUE_LIBS, 'bin/gluepyspark');
const GLUE_SPARK_SUBMIT = Path.join(GLUE_LIBS, 'bin/gluesparksubmit');

function santizeFileName(fileName) {
  if (process.platform === 'win32') {
    return fileName.replace(/[:]/g, '_');
  }
  return fileName;
}

async function getS3Object({ uri, credentials }) {
  const s3 = new AWS.S3({ credentials });
  const { bucket, key } = parseS3Uri(uri);
  const response = await s3
    .getObject({
      Bucket: bucket,
      Key: key,
    })
    .promise();

  if (response.Body) {
    return response.Body.toString('utf8');
  }

  return '{}';
}

async function isExecOnPath(execName) {
  const isWin =
    os.platform().indexOf('darwin') < 0 && os.platform().indexOf('win') > -1;

  const whereCommand = isWin ? 'where' : 'which';

  const out = spawn(whereCommand, [execName], {
    stdio: 'inherit',
    encoding: 'utf8',
    shell: true,
  });

  return new Promise((resolve, reject) => {
    out.on('close', function (code) {
      code === 0 ? resolve(true) : resolve(false);
    });
  });
}

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
  if (!(await isExecOnPath('mvn'))) {
    console.error('you need to install maven before you can continue');
    process.exit(1);
  }

  if (!(await isExecOnPath('python3'))) {
    console.error('you need to install python before you can continue');
    process.exit(1);
  }

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

function getSurveyTypeFromFileName(fileName) {
  return Object.keys(SurveyTypes).find(surveyType => {
    const words = surveyType.split('_');
    let foundAllWords = true;

    words.forEach(_word => {
      let word = _word;
      // if word is number ONLY, add v in front
      // example: 1 becomes v1, 2 becomes v2
      if (word.match(/^\d+$/)) {
        word = `v${word}`;
      }
      if (!fileName.toLowerCase().includes(word.toLowerCase())) {
        foundAllWords = false;
      }
    });

    return foundAllWords;
  });
}

function parseS3Uri(s3Uri) {
  const groups = s3Uri.match(S3_REGEX_PARSER);

  return {
    bucket: groups[1],
    key: groups[2],
  };
}

async function getFileContent(srcRelativePath) {
  return fs.readFile(Path.normalize('./' + srcRelativePath), 'utf8');
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

function getS3PathFromGlueOutput(output) {
  const groups = output.match(OUTPUT_PATH_PARSER);
  if (groups) {
    return groups[1].trim();
  }
}

async function putS3File({ body, uri, credentials }) {
  const { bucket, key } = parseS3Uri(uri);
  const s3 = new AWS.S3({ credentials });
  await s3
    .putObject({
      Bucket: bucket,
      Key: key,
      Body: body,
    })
    .promise();
}

async function main() {
  await ensureSparkHome();
  await ensureGlueLibs();
  const spinner = ora().start('Preparing execution');
  const startTimeFormatted = moment().format('YYYY-MM-DD hh:mm:ss A');

  let stdOutLogFileId;
  let stdErrLogFileId;

  try {
    const surveyType = getSurveyTypeFromFileName(argv.sql);
    if (!surveyType) {
      throw new Error(`Unkonwn survey type for ${argv.sql}`);
    }
    const credentials = await getCredentials();

    const sql = await getFileContent(argv.sql);
    const sqlUri = `s3://doris-survey-reports-${argv.stage.toLowerCase()}/tmp-for-testing/sql-queries/${startTimeFormatted}/${uuid.v4()}.sql`;

    await putS3File({ body: sql, uri: sqlUri, credentials });

    spinner.succeed();
    spinner.start('Executing SQL');

    let outputString = '';
    const sparkStageManager = new SparkStageManager();

    await fs.ensureDir(Path.normalize(`./.spark-logs/${surveyType}`));
    stdOutLogFileId = await fs.open(
      Path.normalize(
        santizeFileName(
          `./.spark-logs/${surveyType}/${startTimeFormatted}.stdout.txt`,
        ),
      ),
      'a',
    );
    stdErrLogFileId = await fs.open(
      Path.normalize(
        santizeFileName(
          `./.spark-logs/${surveyType}/${startTimeFormatted}.stderr.txt`,
        ),
      ),
      'a',
    );

    await fs.chmod(GLUE_SPARK_SUBMIT, 0o777);

    await new Promise((resolve, reject) => {
      const spark = spawn(
        GLUE_SPARK_SUBMIT,
        [
          SPARK_JOB,
          `--tenant_id`,
          argv.tenantId,
          `--stage`,
          argv.stage,
          `--sql`,
          sqlUri,
          `--survey_type`,
          surveyType,
        ],
        {
          // stdio: ['inherit', stdOutLogFileId, stdErrLogFileId],
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
          windowsHide: true,
        },
      );

      spark.stdout.on('data', async data => {
        const strData = data.toString('utf8');
        outputString += strData;
        if (argv.debug) {
          console.log(strData);
        }
        await fs.appendFile(stdOutLogFileId, data, 'utf8');
      });

      spark.stderr.on('data', async data => {
        if (argv.debug) {
          console.error(data.toString('utf8'));
        }
        await fs.appendFile(stdErrLogFileId, data, 'utf8');
        try {
          sparkStageManager.processChunk(data);
        } catch (e) {
          console.error(e.stack);
        }
        if (sparkStageManager.currentStage) {
          spinner.text = `Executing SQL - Stage ${sparkStageManager.currentStage}`;
        }
      });

      spark.on('close', code =>
        code === 0 ? resolve : reject(new Error('process exited ungracefully')),
      );
    });

    const s3OutputUri = getS3PathFromGlueOutput(outputString);

    if (!s3OutputUri) {
      throw new Error('Unable to get S3 output location');
    }

    const outputJson = JSON.parse(
      await getS3Object({ uri: s3OutputUri, credentials }),
    );

    await fs.ensureDir(Path.normalize(`./.json-output/${surveyType}`));
    await fs.writeFile(
      Path.normalize(
        santizeFileName(
          `./.json-output/${surveyType}/${startTimeFormatted}.json`,
        ),
      ),
      JSON.stringify(
        {
          // store s3 output location for reference
          Location: s3OutputUri,
          ...outputJson,
        },
        null,
        2,
      ),
      'utf8',
    );

    spinner.succeed(`Executing SQL`);

    spinner.start('Validating Output');

    const surveyFile = await createSurveyFile({
      reportUri: s3OutputUri,
      unitId: 100654,
      surveyType,
      credentials,
      stage: argv.stage,
    });

    spinner.succeed();

    await fs.ensureDir(Path.normalize(`./.flatfile-output/${surveyType}`));
    await fs.writeFile(
      Path.normalize(
        santizeFileName(
          `./.flatfile-output/${surveyType}/${startTimeFormatted}.txt`,
        ),
      ),
      surveyFile,
      'utf8',
    );
  } catch (e) {
    spinner.fail(e.message);
  } finally {
    if (stdOutLogFileId) {
      await fs.close(stdOutLogFileId);
    }
    if (stdErrLogFileId) {
      await fs.close(stdErrLogFileId);
    }
  }
}

main().catch(e => {
  console.error(e.message);
});
