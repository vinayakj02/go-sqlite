# go-sqlite

Trying to sqlite in go (changed my mind after starting it in c++ lol)

### current status 

It's a project in progress so heres what works rn, 

- `insert <id:int> <username:char[32]> <email:char[255]>` -> insert data into a predefined table ((in memory table))
- `select *` -> fetch from table
- `.dump` -> deletes db
- `.exit` -> exit (automatically flushes to disk )
  

