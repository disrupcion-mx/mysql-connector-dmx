const mysqlssh = require("mysql-ssh");
const mysql = require("mysql2");
const _ = require("lodash");
let db;
const ora = require("ora");
const moment = require("moment");

const _NULL = "______NULL______";

const getMySQLProcedures = () => {
  let base_url = process.cwd() + "/proceduresMethods/";
  let files;
  let found = false;
  try {
    files = require(base_url);
    found = true;
  } catch (error) {
    found = false;
  }
  if (!found) {
    try {
      base_url = process.cwd() + "\\proceduresMethods\\";
      files = require(base_url);
    } catch (error) {
      console.log(
        "proceduresMethods folder not found returning empty procedures"
      );
      return {};
    }
  }
  const methods = {};
  files.forEach((file) => {
    const procedure = require(base_url + file);
    for (const key in procedure) {
      if (procedure.hasOwnProperty(key)) {
        methods[file.replace(".js", "")] = require(base_url + file)[key];
      }
    }
  });
  return methods;
};

/**
 * connect
 * @summary Method that connects db const to MySQL database over ssh
 * @param {Object} ssh ssh connection data
 * @param {String} ssh.host SSH ip address
 * @param {Number} ssh.port SSH Access port
 * @param {String} ssh.user SSH username
 * @param {String} ssh.password SSH password
 * @param {Object} MySQL mysql connection data
 * @param {String} MySQL.host MySQL ip address
 * @param {String} MySQL.user MySQL username
 * @param {String} MySQL.password MySQL password
 * @param {String} MySQL.database MySQL databse name
 * @return {Array} The result of the SQL query (if there is any SELECT)
 *
 *
 * @example
 *
 *     db.connect(ssh, MySQL);
 */
const sshDefault = {
  host: process.env.SSH_HOST,
  port: parseInt(process.env.SSH_PORT),
  user: process.env.SSH_USER,
  password: process.env.SSH_PASSWORD,
};

const MySQLDefault = {
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE_NAME,
};
const connectRaw = async (MySQL = MySQLDefault) => {
  const spinner = ora({
    text: `Connecting to ${MySQL.database} MySQL database without SSH`,
    color: "blue",
    interval: 50,
  }).start();
  spinner.color = "blue";
  return new Promise((resolve, reject) => {
    const connection = mysql.createConnection({
      host: MySQL.host,
      user: MySQL.user,
      database: MySQL.database,
      password: MySQL.password,
      typeCast: function (field, next) {
        if (field.type === "LONGLONG" && field.length === 1) {
          return field.string() === "1";
        }
        if (field.type === "BIT") {
          const buffer = field.buffer();
          if (buffer === null) return false;
          return buffer.readUIntLE(0, 1) === 1;
        }
        if (field.type === "NEWDECIMAL") {
          const value = field.string();
          return value === null ? null : Number(value);
        }
        if (field.type.includes("DATE")) {
          const value = field.string();
          return new moment.utc(value);
        }
        return next();
      },
    });
    connection.connect((err) => {
      if (err) {
        console.error("error connecting: " + err.stack);
        setTimeout(connectRaw(MySQL), 2000);
        reject(err.stack);
      }
      db = connection;
      spinner.succeed(
        `Connected successfully to ${MySQL.database} MySQL database without SSH`
      );
      resolve();
    });
    connection.on("error", (err) => {
      console.log("db error", err);
      if (err.code === "PROTOCOL_CONNECTION_LOST") {
        // Connection to the MySQL server is usually
        connectRaw(MySQL); // lost due to either server restart, or a
      } else {
        // connnection idle timeout (the wait_timeout
        throw err; // server variable configures this)
      }
    });
  });
};
const connect = async (ssh = sshDefault, MySQL = MySQLDefault) => {
  if (!ssh.host) return;
  const spinner = ora({
    text: `Connecting to ${MySQL.database} MySQL database`,
    color: "blue",
    interval: 50,
  }).start();
  spinner.color = "blue";
  return new Promise((resolve, reject) => {
    mysqlssh
      .connect(ssh, {
        ...MySQL,
        typeCast: function (field, next) {
          if (field.type === "LONGLONG" && field.length === 1) {
            return field.string() === "1";
          }
          if (field.type === "BIT") {
            const buffer = field.buffer();
            if (buffer === null) return false;
            return buffer.readUIntLE(0, 1) === 1;
          }
          if (field.type === "NEWDECIMAL") {
            const value = field.string();
            return value === null ? null : Number(value);
          }
          if (field.type.includes("DATE")) {
            const value = field.string();
            return new moment.utc(value);
          }
          return next();
        },
      })
      .then((client) => {
        db = client;
        spinner.succeed(
          `Connected successfully to ${MySQL.database} MySQL database`
        );
        resolve();
      })
      .catch((err) => {
        spinner.fail("Could not connect to database");
        reject(err);
      });
  });
};

const replaceAll = (str, find, replace) => {
  return str.replace(new RegExp(find, "g"), replace);
};
const parseNodeToMysql = (args) => {
  let str = "";
  let comma = "";
  let error = undefined;
  //Remove procedure name
  args.shift();
  args.forEach((arg) => {
    if (!error) {
      switch (typeof arg) {
        case "string":
          if (arg === _NULL) str += `${comma}null`;
          else {
            arg = replaceAll(arg, `"`, `\\"`);
            arg = replaceAll(arg, `'`, `\\'`);
            arg = replaceAll(arg, `\``, `\\\``);
            arg = replaceAll(arg, `\´`, `\\´`);
            str += `${comma}"${arg}"`;
          }
          break;
        case "number":
          str += `${comma}${arg}`;
          break;
        case "boolean":
          const bool = arg ? 1 : 0;
          str += `${comma}${bool}`;
          break;
        case "undefined":
          error =
            "Error: You tried to send an undefined value to MySQL Procedure\nParameter: " +
            args;
          break;
        case "object":
          if (arg === null)
            error = new Error(
              "Error: You tried to send a null value to MySQL Procedure\nParameter: " +
                args
            );
          else if (arg.constructor.name === "Moment")
            str += `${comma}"${arg.format("YYYY-MM-DD HH:mm:ss")}"`;
          else if (arg != null && arg.constructor.name === "Object")
            error = new Error(
              "Error: You tried to send an object value to MySQL Procedure\nParameter: " +
                args
            );
          else
            error = new Error(
              "Error: You tried to send an array value to MySQL Procedure\nParameter: " +
                args
            );
          break;
        default:
          error = new Error(
            "Error: You tried to send either a function, symbol, bigInt\nParameter: " +
              args
          );
          break;
      }
      comma = ",";
    }
  });
  if (error) str = undefined;
  return { error, str };
};

/**
 * Method that calls a MySQL Procedure
 *
 * @param {String} procedureName - Name of the procedure to call
 * @param {...*} var_args - Any amount of parameters
 * @return {Array} The result of the procedure query (if there is any SELECT)
 *
 * @example
 *
 *     db.query('GetAllUsers', 1, true, 'obra');
 */

const queryProcedure = async (...args) => {
  return new Promise((resolve, reject) => {
    const parsedArgs = parseNodeToMysql(_.cloneDeep(args));
    if (!parsedArgs.error) {
      db.promise()
        .query(`CALL ${args[0]}(${parsedArgs.str})`)
        .then((res) => {
          resolve(res[0][0]);
        })
        .catch((err) => {
          reject(err);
        });
    } else {
      reject(parsedArgs.error + `, on procedure "${args[0]}"`);
    }
  });
};

/**
 * Method that calls a MySQL SCRIPT
 * @summary DO NOT ADD user_input TO SCRIPT, MAY LEAD TO SQL INJECTION
 *
 *
 * @param {String} str - SQL code as String
 * @return {Array} The result of the SQL query (if there is any SELECT)
 *
 * @example
 *
 *     db.rawQuery('SELECT * FROM Usuario');
 */

const rawQuery = async (str) => {
  return new Promise((resolve, reject) => {
    db.promise()
      .query(str)
      .then((res) => {
        resolve(res[0]);
      })
      .catch((err) => {
        reject(err);
      });
  });
};

const emptyTestDatabase = async () => {
  return new Promise((resolve, reject) => {
    if (!db.config.database.includes("test"))
      return reject(
        "CRITICAL WARNING, YOU TRIED TO TRUNCATE A NON-TESTING DATABASE"
      );
    queryProcedure("getTruncateCommands").then(async (commands) => {
      const promises = [];
      commands.forEach((command) => {
        const promise = new Promise((resolveCommand, rejectCommand) => {
          rawQuery(command.str).then(() => {
            resolveCommand();
          });
        });
        promises.push(promise);
      });
      Promise.all(promises).then(() => {
        resolve();
      });
    });
  });
};

module.exports = {
  connect,
  connectRaw,
  queryProcedure,
  rawQuery,
  emptyTestDatabase,
  procedures: getMySQLProcedures(),
  _NULL,
};
