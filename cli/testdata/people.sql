-- Create a table.
create table people (_id id, name string, age int);

-- Insert some values.
insert into people values (1, 'Amy', 42), (2, 'Bob', 27), (3, 'Carl', 33);

-- Get all rows from the table.
select * from people;

-- Mix in a meta-command to show that both are supported
-- in the include file.
\echo mix in a meta command
