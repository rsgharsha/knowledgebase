-- Insert values into students table
INSERT INTO students
    (`ID`, `NAME`, `GENDER`, `AGE`)
VALUES
    (1, 'Revathi', 	'F', 	22),
    (2, 'Devi', 	'F', 	23),
    (3, 'Sesha', 	'M', 	24),
    (4, 'Harsha', 	'M', 	24),
    (5, 'Giri',  	'M', 	23),
    (6, 'Rupa', 	 'F',    21)
;
-- retrive data from students table
select * from students;