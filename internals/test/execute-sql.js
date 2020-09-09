const AWS = require('aws-sdk');
const ssh2 = require('ssh2');
const net = require('net');
const fs = require('fs-extra');
const Path = require('path');
const uuid = require('uuid');
const ora = require('ora');
const { argv } = require('yargs');
const moment = require('moment');
const SurveyTypes = require('./survey-types');
const SparkStageManager = require('./spark-stage-manager');

const OUTPUT_PATH_PARSER = /##OUTPUT##: (s3:\/\/[^\s]+)/;
const S3_REGEX_PARSER = /s3:\/\/([^\/]+)\/?(.*)/;

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

async function asPromise(method, ...args) {
  return new Promise((resolve, reject) => {
    method(...args, (err, ...args) => {
      if (err) {
        reject(err);
      } else if (args.length < 2) {
        resolve(args[0]);
      } else {
        resolve(args);
      }
    });
  });
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

function getSurveyYearFromFileName(fileName) {
  const match = fileName.match(/.*src\/(\d+)\/.*/);
  if (!match) {
    throw new Error('Survey year could not be determined from path');
  }
  return Number(match[1]);
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

async function getPrivateKey({ credentials }) {
  const ssm = new AWS.SSM({ region: 'us-east-1', credentials });
  const result = await ssm
    .getParameter({
      Name: '/doris/shared/glue/dev-endpoint-private-key',
      WithDecryption: true,
    })
    .promise();

  return result.Parameter.Value;
}

async function getDevEndpointHost({ credentials }) {
  const glue = new AWS.Glue({ region: 'us-east-1', credentials });
  try {
    const result = await glue
      .getDevEndpoint({
        EndpointName: 'doris-test-endpoint-DEV',
      })
      .promise();

    return result.DevEndpoint.PublicAddress;
  } catch (e) {
    throw new Error(
      'Glue Dev Endpoint not started. Run zeppelin automation to enable.',
    );
  }
}

async function exec(conn, statement, options = {}) {
  return new Promise((resolve, reject) => {
    conn.exec(statement, function (err, stream) {
      if (err) {
        return reject(err);
      }
      stream
        .on('close', function (code, signal) {
          resolve();
        })
        .on('error', e => {
          reject(e);
        })
        .on('data', function (data) {
          if (options.onStdout) {
            options.onStdout(data);
          }
        })
        .stderr.on('data', function (data) {
          if (options.onStderr) {
            options.onStderr(data);
          }
        });
    });
  });
}

async function connect({ host, username, privateKey }) {
  const conn = new ssh2.Client();
  return new Promise(async (resolve, reject) => {
    conn.on('ready', () => {
      resolve(conn);
    });
    conn.on('error', e => reject(e));
    conn.connect({
      host: host,
      username,
      privateKey: privateKey,
    });
  });
}

async function forward(connection, { fromPort, toPort, toHost }) {
  return new Promise((resolve, reject) => {
    const server = net.createServer(socket => {
      connection.forwardOut(
        'localhost',
        fromPort,
        toHost || 'localhost',
        toPort,
        (error, stream) => {
          if (error) {
            return reject(error);
          }
          socket.pipe(stream);
          stream.pipe(socket);
        },
      );
    });

    server.listen(fromPort, 'localhost', () => {
      return resolve(() => server.close());
    });
  });
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

async function putFile(conn, localPath, remotePath) {
  const sftp = await new Promise((resolve, reject) =>
    conn.sftp((err, sftp) => (err ? reject(err) : resolve(sftp))),
  );
  await new Promise((resolve, reject) =>
    sftp.fastPut(localPath, remotePath, err => (err ? reject(err) : resolve())),
  );
}

async function createSurveyFile({
  reportUri,
  unitId,
  surveyType,
  stage = 'DEV',
  credentials,
}) {
  const lambda = new AWS.Lambda({ region: 'us-east-1', credentials });
  const response = await lambda
    .invoke({
      FunctionName: `iris-connector-doris-2019-${stage}-createSurveyFile`,
      Payload: JSON.stringify({
        surveyFileId: uuid.v4(),
        reportUri,
        unitId,
        surveyDefinitionType: surveyType,
      }),
    })
    .promise();

  const body = JSON.parse(response.Payload || '{}');

  if (body.errorMessage) {
    let errorMessage = body.errorMessage;
    if (body.stackTrace) {
      errorMessage = body.stackTrace.join('\n');
    }
    throw new Error(errorMessage);
  } else {
    return body.surveyFile;
  }
}

async function main() {
  const spinner = ora().start('Preparing execution');
  const startTimeFormatted = moment().format('YYYY-MM-DD hh:mm:ss A');

  let conn;
  let closeForwardConnection;
  let stdOutLogFileId;
  let stdErrLogFileId;

  try {
    const surveyType = getSurveyTypeFromFileName(argv.sql);
    const surveyYear = getSurveyYearFromFileName(argv.sql);
    if (!surveyType) {
      throw new Error(`Unkonwn survey type for ${argv.sql}`);
    }
    const credentials = await getCredentials();
    conn = await connect({
      username: 'glue',
      host: await getDevEndpointHost({ credentials }),
      privateKey: await getPrivateKey({ credentials }),
    });

    closeForwardConnection = await forward(conn, {
      fromPort: 18080,
      toHost: '169.254.76.1',
      toPort: 18080,
    });

    await putFile(
      conn,
      Path.resolve(__dirname, '../pyspark/spark-executor.py'),
      '/home/glue/job.py',
    );

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

    await exec(
      conn,
      `/usr/bin/gluepython3 /home/glue/job.py --tenant_id="${argv.tenantId}" --stage="${argv.stage}" --sql="${sqlUri}" --survey_type="${surveyType}" --year="${surveyYear}" --user_id="${argv.userId}" --email="${argv.email}"`,
      {
        onStdout: async data => {
          const strData = data.toString('utf8');
          outputString += strData;
          if (argv.debug) {
            console.log(strData);
          }
          await fs.appendFile(stdOutLogFileId, data, 'utf8');
        },
        onStderr: async data => {
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
        },
      },
    );

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
    if (closeForwardConnection) {
      closeForwardConnection();
    }
    if (conn) {
      conn.end();
    }
    if (stdOutLogFileId) {
      await fs.close(stdOutLogFileId);
    }
    if (stdErrLogFileId) {
      await fs.close(stdErrLogFileId);
    }
  }
}

main().catch(() => null);
