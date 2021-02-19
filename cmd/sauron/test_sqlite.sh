#!/bin/bash
db="poc2.db"
sqlite3 ${db} <<EOF
create table λbits (field ,row , column , timestamp);
create table λbsi (field string,column bigint , val bigint);

CREATE VIEW λcolumns as
	select distinct column from λbits
        UNION
	select distinct column from λbsi;
INSERT INTO λbsi VALUES( 'luminosity',1, 1);
INSERT INTO λbsi VALUES( 'luminosity',2, 2);
INSERT INTO λbsi VALUES( 'luminosity',3, 4);
INSERT INTO λbsi VALUES( 'luminosity',4, 5);
INSERT INTO λbsi VALUES( 'luminosity',5, 4);
INSERT INTO λbsi VALUES( 'luminosity',6, 3);
INSERT INTO λbsi VALUES( 'luminosity',7, 2);
INSERT INTO λbsi VALUES( 'luminosity',8, 1);

INSERT INTO λbits (field,row,column)VALUES( 'color','red',1);
INSERT INTO λbits (field,row,column)VALUES( 'color','red',2);
INSERT INTO λbits (field,row,column)VALUES( 'color','red',3);
INSERT INTO λbits (field,row,column)VALUES( 'color','red',4);
INSERT INTO λbits (field,row,column)VALUES( 'color','red',5);
INSERT INTO λbits (field,row,column)VALUES( 'color','green',2);
INSERT INTO λbits (field,row,column)VALUES( 'color','green',4);
INSERT INTO λbits (field,row,column)VALUES( 'color','green',6);
INSERT INTO λbits (field,row,column)VALUES( 'color','green',8);
INSERT INTO λbits (field,row,column)VALUES( 'color','yellow',1);
INSERT INTO λbits (field,row,column)VALUES( 'color','yellow',3);
INSERT INTO λbits (field,row,column)VALUES( 'color','yellow',5);
INSERT INTO λbits (field,row,column)VALUES( 'color','yellow',7);
INSERT INTO λbits (field,row,column)VALUES( 'color','orange',5);
INSERT INTO λbits (field,row,column)VALUES( 'color','orange',6);
INSERT INTO λbits (field,row,column)VALUES( 'color','orange',7);
INSERT INTO λbits (field,row,column)VALUES( 'color','orange',8);
INSERT INTO λbits (field,row,column)VALUES( 'rating','1',8);
INSERT INTO λbits (field,row,column)VALUES( 'rating','1',7);
INSERT INTO λbits (field,row,column)VALUES( 'rating','1',10);
INSERT INTO λbits (field,row,column)VALUES( 'rating','2',6);
INSERT INTO λbits (field,row,column)VALUES( 'rating','2',5);
INSERT INTO λbits (field,row,column)VALUES( 'rating','3',4);
INSERT INTO λbits (field,row,column)VALUES( 'rating','4',3);
INSERT INTO λbits (field,row,column)VALUES( 'rating','4',2);

INSERT INTO λbits (field,row,column,timestamp)VALUES( 'event','1',5,'2021-02-05 01:00');
INSERT INTO λbits (field,row,column,timestamp)VALUES( 'event','1',5,'2021-02-05 02:00');
INSERT INTO λbits (field,row,column,timestamp)VALUES( 'event','1',4,'2021-02-05 02:05');
INSERT INTO λbits (field,row,column,timestamp)VALUES( 'event','1',3,'2021-02-05 03:00');
INSERT INTO λbits (field,row,column,timestamp)VALUES( 'event','1',2,'2021-02-05 04:00');
EOF
