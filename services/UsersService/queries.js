export const CREATE_TABLE_USER = `
  CREATE TABLE IF NOT EXISTS users (
    uuid varchar(200) primary key,
    email varchar(200)
  )
`

export const INSERT_USER = `
    INSERT INTO users (uuid, email)
    VALUES (?, ?)
`;

export const SELECT_USER = `
  SELECT uuid FROM users WHERE
  email=?
`;