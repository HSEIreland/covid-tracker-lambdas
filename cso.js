const Client = require('ssh2-sftp-client')
const SQL = require('@nearform/sql')
const openpgp = require('openpgp')
const { getCsoConfig, getDatabase, runIfDev } = require('./utils')

async function getCheckIns(client) {
  const query = SQL`
    SELECT created_at, sex, age_range, locality, ok, payload
    FROM check_ins
    WHERE created_at = DATE 'yesterday'`

  const results = [['age_range', 'sex', 'locality', 'feeling_ok']]

  for (let i = 1; i <= 28; i++) {
    results[0].push(
      `symptom_fever_${i}`,
      `symptom_cough_${i}`,
      `symptom_breath_${i}`,
      `symptom_flu_${i}`,
      `covid_status_${i}`,
      `date_${i}`
    )
  }

  const { rows } = await client.query(query)

  for (const { age_range, sex, locality, ok, payload } of rows) {
    const result = [age_range, sex, locality, ok]

    for (let i = 0; i < 28; i++) {
      if (payload.data[i]) {
        const { fever, cough, breath, flu, status, date } = payload.data[i]

        result.push(
          /(true|1|y)/i.test(fever),
          /(true|1|y)/i.test(cough),
          /(true|1|y)/i.test(breath),
          /(true|1|y)/i.test(flu),
          status,
          date
        )
      } else {
        result.push('', '', '', '', '', '', '', '')
      }
    }

    results.push(result)
  }

  return results
}

async function clearCheckIns(client) {
  const query = SQL`
    DELETE FROM check_ins
    WHERE created_at <= CURRENT_DATE - INTERVAL '2 days'`

  await client.query(query)
}

exports.handler = async function () {
  const { publicKey, host, port, username, password, checkInPath } = await getCsoConfig()

  const client = await getDatabase()
  const checkIns = await getCheckIns(client)
  const keyResult = await openpgp.key.readArmored(publicKey)

  await clearCheckIns()

  const { data: encryptedCheckIns } = await openpgp.encrypt({
    message: openpgp.message.fromText(checkIns.join('\n')),
    publicKeys: keyResult.keys
  })

  const now = new Date()
  const date = `${now.getFullYear()}-${now.getMonth() + 1}-${now.getDate()}`
  const sftp = new Client()

  await sftp.connect({
    host,
    port,
    username,
    password,
    algorithms: {
      serverHostKey: ['ssh-rsa', 'ssh-dss']
    }
  })

  await sftp.put(
    Buffer.from(encryptedCheckIns, 'utf8'),
    `${checkInPath}/checkins-${date}-1.csv.gpg`
  )

  return {
    checkIns: {
      raw: checkIns,
      encrypted: encryptedCheckIns
    }
  }
}

runIfDev(exports.handler)
