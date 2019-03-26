const rethinkdb = require('rethinkdb')

if (process.env.dev) {
  require('dotenv').config()
}

const { databaseHost } = process.env
const authenticate = () => {
  return rethinkdb.connect({ host: databaseHost })
}

const logout = connection => connection.close()

const set = (repo, values, sha, token) => {
  const ref = `${token}/${repo}`.replace(/\W/g, '_')
  authenticate().then(conn => {
    values[0].sha = sha
    rethinkdb
      .tableList()
      .run(conn)
      .then(arrTables => {
        if (arrTables.indexOf(ref) === -1) {
          rethinkdb
            .tableCreate(ref)
            .run(conn)
            .then(() => {
              rethinkdb
                .table(ref)
                .insert({ ...values })
                .run(conn)
                .then(logout(conn))
            })
        } else {
          rethinkdb
            .table(ref)
            .insert({ ...values })
            .run(conn)
            .then(logout(conn))
        }
      })
  })
}

const get = (repo, token) => {
  const ref = `${token}/${repo}`.replace(/\W/g, '_')
  authenticate().then(conn => {
    console.log('connected!')
    rethinkdb
      .table(ref)
      .getAll()
      .limit(1)
      .run(conn)
      .then(cursor => {
        cursor.next().then(snapshot => {
          console.log(snapshot)
          const object = snapshot.val()
          if (!object) return []
          const values = Object.values(object)[0]
          logout(conn)
          return values
        })
      })
  })
}

module.exports = { set, get }
