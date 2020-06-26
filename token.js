const jwt = require('jsonwebtoken')
const SQL = require('@nearform/sql')
const { getDatabase, getJwtSecret, runIfDev } = require('./utils')

exports.handler = async function (event) {
  const sql = SQL`
    INSERT INTO tokens (type)
    VALUES (${event.type || 'push'})
    RETURNING id`
  
  const secret = await getJwtSecret()
  const client = await getDatabase()
  const { rowCount, rows } = await client.query(sql)

  if (rowCount === 0) {
    throw new Error('Unable to create token')
  }

  const [{ id }] = rows

  return jwt.sign({ id }, secret, { expiresIn: '1y' })
}

runIfDev(exports.handler)
