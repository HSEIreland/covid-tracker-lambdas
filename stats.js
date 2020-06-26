const AWS = require('aws-sdk')
const SQL = require('@nearform/sql')
const fetch = require('node-fetch')
const { getAssetsBucket, getDatabase, getStatsUrl, runIfDev } = require('./utils')

async function getGeoHiveStats(serviceName, params) {
  const defaultParams = {
    f: 'json',
    where: '1=1',
    returnGeometry: false,
    outFields: '*'
  }

  const queryParams = Object.assign({}, defaultParams, params)
  const query = Object.keys(queryParams)
    .map(k => `${k}=${encodeURI(queryParams[k])}`)
    .join('&')

  const servicesUrl = await getStatsUrl()
  const url = `${servicesUrl}/${serviceName}/FeatureServer/0/query?${query}`

  const response = await fetch(url)
  const responseJson = await response.json()

  if (responseJson.features.length === 1) {
    return responseJson.features[0].attributes
  }

  return responseJson.features.map(f => f.attributes)
}

async function getStatistics() {
  const data = await getGeoHiveStats('CovidStatisticsProfileHPSCIrelandOpenData')
  const current = data[data.length - 1]

  return {
    statistics: {
      confirmed: current.TotalConfirmedCovidCases,
      deaths: current.TotalCovidDeaths,
      recovered: current.TotalCovidRecovered,
      hospitalised: current.HospitalisedCovidCases,
      requiredICU: current.RequiringICUCovidCases,
      transmission: {
        community: current.CommunityTransmission,
        closeContact: current.CloseContact,
        travelAbroad: current.TravelAbroad
      },
      lastUpdated: {
        stats: new Date(current.Date),
        profile: new Date(current.StatisticsProfileDate)
      }
    },
    chart: data.map(item => ([
      new Date(item.Date),
      item.ConfirmedCovidCases
    ])),
    currentCases: data.map(item => ([
      new Date(item.Date),
      item.ConfirmedCovidCases
    ])),
    hospitalised: data.map((item, index) => {
      const previous = index > 0 && data[index - 1]

      return [
        new Date(item.Date),
        Math.max(0, previous ? item.HospitalisedCovidCases - previous.HospitalisedCovidCases : item.HospitalisedCovidCases)
      ]
    }),
    requiredICU: data.map((item, index) => {
      const previous = index > 0 && data[index - 1]

      return [
        new Date(item.Date),
        Math.max(0, previous ? item.RequiringICUCovidCases - previous.RequiringICUCovidCases : item.RequiringICUCovidCases)
      ]
    })
  }
}

async function getCounties() {
  const data = await getGeoHiveStats('CovidCountyStatisticsHPSCIrelandOsiView')

  return data.map(item => ({
    county: item.CountyName,
    cases: item.ConfirmedCovidCases
  }))
}

async function getCheckIns(client) {
  const sql = SQL`
    SELECT
      COUNT(*) AS total,
      COUNT(ok) FILTER (WHERE ok) AS ok
    FROM check_ins
    WHERE created_at = CURRENT_DATE`

  const { rows } = await client.query(sql)
  const [{ total, ok }] = rows

  return {
    total: Number(total),
    ok: Number(ok)
  }
}

async function getInstalls(client) {
  const sql = SQL`
    SELECT
      created_at::DATE AS day,
      SUM(COUNT(id)) OVER (ORDER BY created_at::DATE) AS count
    FROM registrations
    GROUP BY created_at::DATE`

  const { rows } = await client.query(sql)

  return rows.map(({ day, count }) => ([new Date(day), Number(count)]))
}

async function getStatsBody(client) {
  const checkIns = await getCheckIns(client)
  const installs = await getInstalls(client)
  const statistics = await getStatistics()
  const counties = await getCounties()

  return {
    generatedAt: new Date(),
    checkIns,
    installs,
    counties,
    ...statistics
  }
}

exports.handler = async function () {
  const s3 = new AWS.S3({ region: process.env.AWS_REGION })
  const client = await getDatabase()
  const bucket = await getAssetsBucket()
  const stats = JSON.stringify(await getStatsBody(client))

  const statsObject = {
    ACL: 'private',
    Body: Buffer.from(stats),
    Bucket: bucket,
    ContentType: 'application/json',
    Key: 'stats.json'
  }

  await s3.putObject(statsObject).promise()

  return stats
}

runIfDev(exports.handler)
