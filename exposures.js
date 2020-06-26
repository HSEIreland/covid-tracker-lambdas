const AWS = require('aws-sdk')
const archiver = require('archiver')
const crypto = require('crypto')
const protobuf = require('protobufjs')
const SQL = require('@nearform/sql')
const { getAssetsBucket, getDatabase, getExposuresConfig, runIfDev } = require('./utils')

async function clearExpiredExposures(client) {
  const query = SQL`
    DELETE FROM exposures
    WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '14 days'
  `

  await client.query(query)
}

async function clearExpiredFiles(client, s3, bucket) {
  const query = SQL`
    DELETE FROM exposure_export_files
    WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '14 days'
    RETURNING path
  `

  const promises = []
  const { rows } = await client.query(query)

  for (const { path } of rows) {
    const fileObject = {
      Bucket: bucket,
      Key: path
    }

    promises.push(s3.deleteObject(fileObject).promise())
  }

  await Promise.all(promises)
}

async function uploadFile(client, s3, bucket) {
  const firstExposureId = await getFirstExposureId(client)
  const exposures = await getExposures(client, firstExposureId)

  if (exposures.length > 0) {
    const now = new Date()
    const path = `exposures/${now.getTime()}.zip`
    const lastExposureId = exposures.reduce((max, { id }) => id > max ? id : max, firstExposureId)

    const exportFileObject = {
      ACL: 'private',
      Body: await createExportFile(exposures, 1, 1),
      Bucket: bucket,
      ContentType: 'application/zip',
      Key: path
    }

    await s3.putObject(exportFileObject).promise()

    const query = SQL`
      INSERT INTO exposure_export_files (path, exposure_count, last_exposure_id)
      VALUES (${path}, ${exposures.length}, ${lastExposureId})
    `

    await client.query(query)
  }
}

async function getFirstExposureId(client) {
  const query = SQL`
    SELECT COALESCE(MAX(last_exposure_id), 0) AS "firstExposureId"
    FROM exposure_export_files
  `

  const { rows } = await client.query(query)
  const [{ firstExposureId }] = rows

  return firstExposureId
}

async function getExposures(client, since) {
  const query = SQL`
    SELECT id, created_at, key_data, rolling_period, rolling_start_number, transmission_risk_level
    FROM exposures
    WHERE id > ${since}
    ORDER BY key_data ASC
  `

  const { rows } = await client.query(query)

  return rows
}

function createExportFile(exposures, batchNum, batchSize) {
  return new Promise(async resolve => {
    const { privateKey, ...signatureInfoPayload } = await getExposuresConfig()

    const root = await protobuf.load('exposures.proto')
    const tekExport = root.lookupType('TemporaryExposureKeyExport')
    const signatureList = root.lookupType('TEKSignatureList')
    const sign = crypto.createSign('sha256')

    const startDate = exposures.reduce((current, { created_at }) => current === null || new Date(created_at) < current ? new Date(created_at) : current, null)
    const endDate = exposures.reduce((current, { created_at }) => current === null || new Date(created_at) > current ? new Date(created_at) : current, null)

    const tekExportPayload = {
      startTimestamp: Math.floor(startDate / 1000),
      endTimestamp: Math.floor(endDate / 1000),
      region: 'IE',
      batchNum,
      batchSize,
      signatureInfos: [signatureInfoPayload],
      keys: exposures.map(({ key_data, rolling_start_number, transmission_risk_level, rolling_period }) => ({
        keyData: key_data,
        rollingStartIntervalNumber: rolling_start_number,
        transmissionRiskLevel: transmission_risk_level,
        rollingPeriod: rolling_period
      }))
    }

    const tekExportMessage = tekExport.create(tekExportPayload)
    const tekExportEncoded = tekExport.encode(tekExportMessage).finish()

    const tekExportData = Buffer.concat([
      Buffer.from('EK Export v1'.padEnd(16), 'utf8'),
      tekExportEncoded
    ])

    sign.update(tekExportData)
    sign.end()

    const signature = sign.sign({
      key: privateKey,
      dsaEncoding: 'der'
    })

    const signatureListPayload = {
      signatures: [
        {
          signatureInfo: signatureInfoPayload,
          batchNum,
          batchSize,
          signature
        }
      ]
    }

    const signatureListMessage = signatureList.create(signatureListPayload)
    const signatureListEncoded = signatureList.encode(signatureListMessage).finish()

    const archive = archiver('zip')
    let output = Buffer.alloc(0)

    archive.on('data', data => {
      output = Buffer.concat([output, data])
    })

    archive.on('finish', () => {
      resolve(output)
    })

    archive.append(tekExportData, { name: 'export.bin' })
    archive.append(signatureListEncoded, { name: 'export.sig' })
    archive.finalize()
  })
}

exports.handler = async function () {
  const s3 = new AWS.S3({ region: process.env.AWS_REGION })
  const client = await getDatabase()
  const bucket = await getAssetsBucket()

  await uploadFile(client, s3, bucket)
  await clearExpiredExposures(client)
  await clearExpiredFiles(client, s3, bucket)

  return true
}

runIfDev(exports.handler)
