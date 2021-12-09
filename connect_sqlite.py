import os.path
import sqlite3
import pandas as pd

# Connects to an existing database file in the current directory
# If the file does not exist, it creates it in the current directory
if os.path.exists("test.db"):
	os.remove("test.db")
	
db_connect = sqlite3.connect('test.db')

# Instantiate cursor object for executing queries
cursor = db_connect.cursor()

# String variable for passing queries to cursor
tables = (
	"""
    CREATE TABLE Department(
    deptID INT,
    deptName VARCHAR(50) 
    	CHECK(deptName LIKE 'Department%'),
    chairName VARCHAR(50),
    numFaculty INT
    	CHECK (numFaculty > 0),
    PRIMARY KEY(deptID)
    );
    """, 
    """
    CREATE TABLE Major(
    majorID CHAR(3) 
    	CHECK(length(majorID) = 3),
    majorName VARCHAR(100),
    deptID INT,
    PRIMARY KEY(majorID),
    FOREIGN KEY(deptID)
    	REFERENCES Department(deptID)
    		ON DELETE CASCADE
    		ON UPDATE CASCADE
    );
    """,
    """
    CREATE TABLE Student(
    sID INT,
    sName VARCHAR(100),
    sInit VARCHAR(10)
    	CHECK(length(sInit) > 1),
	PRIMARY KEY(sID)    
    );
    """,
    """
    CREATE TABLE Event(
    eventID INT,
    eventName VARCHAR(100),
    sDate DATE default sysdate,
    fDate DATE,
    recordCreationDate DATE default sysdate,
    PRIMARY KEY(eventID),
    CHECK (sDate > recordCreationDate),
    CHECK (fDate > recordCreationDate),
    CHECK (fDate >= sDate)
    );
    """,
    """
    CREATE TABLE DeptEvent(
    deptID INT,
    eventID INT,
    PRIMARY KEY(deptID,eventID),
    FOREIGN KEY(deptID)
    	REFERENCES Department(deptID)
    		ON DELETE CASCADE
    		ON UPDATE CASCADE,
   FOREIGN KEY(eventID)
    	REFERENCES Event(eventID)
    		ON DELETE CASCADE
    		ON UPDATE CASCADE
	);
    """,  
    """
    CREATE TABLE DeptMajor(
    deptID INT,
    majorID CHAR(3)
    	CHECK(length(majorID) = 3),
    PRIMARY KEY(deptID,majorID),
    FOREIGN KEY(deptID)
    	REFERENCES Department(deptID)
    		ON DELETE CASCADE
    		ON UPDATE CASCADE,
    FOREIGN KEY(majorID)
    	REFERENCES Major(majorID)
    		ON DELETE CASCADE
    		ON UPDATE CASCADE
    );
    """,
    """
    CREATE TABLE StudentEvent(
    sID INT,
    eventID INT,
    PRIMARY KEY(sID,eventID),
    FOREIGN KEY(sID)
    	REFERENCES Student(sID)
    		ON DELETE CASCADE
    		ON UPDATE CASCADE,
    FOREIGN KEY(eventID)
    	REFERENCES Event(eventID)
    		ON DELETE CASCADE
    		ON UPDATE CASCADE
    );
    """,
    """
    CREATE TABLE StudentMajor(
    sID INT,
    majorID CHAR(3)
    	CHECK(length(majorID) = 3),
    PRIMARY KEY(sID,majorID),
    FOREIGN KEY(sID)
    	REFERENCES Student(sID)
    		ON DELETE CASCADE
    		ON UPDATE CASCADE,
    FOREIGN KEY(majorID)
    	REFERENCES Major(majorID)
    		ON DELETE CASCADE
    		ON UPDATE CASCADE
    );    
    """
)



# Execute query, the result is stored in cursor
for t in tables:
	cursor.execute(t)
	
# Insert row into table
inserts = ( 
	"""
    INSERT INTO Department
    VALUES (100, "Department of Mathematics", "Yamudda Anyafadda",25);
    """ ,
	"""
    INSERT INTO Department
    VALUES (200, "Department of Physics", "Elon Musk",18);
    """ ,
	"""
    INSERT INTO Department
    VALUES (300, "Department of Computer Science", "Ada Lovelace",51);
    """ ,
	"""
    INSERT INTO Department
    VALUES (400, "Department of Philosophy", "Prince Harry",12);
    """ ,
	"""
    INSERT INTO Department
    VALUES (500, "Department of Biology", "Ekans Fennekin",3);
    """ ,
	"""
    INSERT INTO Major
    VALUES ("PHI", "Philosophy", 4);
    """ ,
	"""
    INSERT INTO Major
    VALUES ("APY", "Astrophysics", 2);
    """ ,
	"""
    INSERT INTO Major
    VALUES ("TPG", "Topology", 1);
    """ ,
	"""
    INSERT INTO Major
    VALUES ("CNS", "Computational Neuroscience", 3);
    """ ,
	"""
    INSERT INTO Major
    VALUES ("BIO", "Biology", 5);
    """ ,
	"""
    INSERT INTO Student
    VALUES (1, "Eitan Raitses", "ER");
    """ ,
	"""
    INSERT INTO Student
    VALUES (2, "John G. Goomba", "JGG");
    """ ,
	"""
    INSERT INTO Student
    VALUES (3, "Anna B. Goomba", "ABG");
    """ ,
	"""
    INSERT INTO Student
    VALUES (4, "Phil Phillerson", "PP");
    """ ,
	"""
    INSERT INTO Student
    VALUES (5, "Randall K. Orton", "RKO");
    """ ,
    """
   	INSERT INTO Event
   	VALUES (1001,"Abstract Mathematics Seminar",'2021/12/30','2022/01/01',NULL);
    """ ,
    """
   	INSERT INTO Event
   	VALUES (1002,"Discussion on Relativity",'2021/12/23', '2021/12/24',NULL);
    """ ,
    """
   	INSERT INTO Event
   	VALUES (1003,"Philosophy of Emotions",'2022/02/01','2022/02/05',NULL);
    """ ,
    """
   	INSERT INTO Event
   	VALUES (1004,"Artificial General Intelligence",'2021/12/29','2021/12/31',NULL);
    """ ,
    """
   	INSERT INTO Event
   	VALUES (1005,"Biology of Reptiles",'2022/04/01','2022/04/01',NULL);
    """ ,
    """
   	INSERT INTO DeptEvent
   	VALUES (100,1001);
    """ ,
    """
   	INSERT INTO DeptEvent
   	VALUES (200,1002);
    """ ,
    """
   	INSERT INTO DeptEvent
   	VALUES (400,1003);
    """ ,
    """
   	INSERT INTO DeptEvent
   	VALUES (300,1004);
    """ ,
    """
   	INSERT INTO DeptEvent
   	VALUES (500,1005);
    """ ,
    """
   	INSERT INTO DeptMajor
   	VALUES (100,"TPG");
    """ ,
    """
   	INSERT INTO DeptMajor
   	VALUES (200,"APY");
    """ ,
    """
   	INSERT INTO DeptMajor
   	VALUES (300,"CNS");
    """ ,
    """
   	INSERT INTO DeptMajor
   	VALUES (400,"PHI");
    """ ,
    """
   	INSERT INTO DeptMajor
   	VALUES (500,"BIO");
    """ ,
    """
   	INSERT INTO StudentEvent
   	VALUES (1,1001);
    """ ,
    """
   	INSERT INTO StudentEvent
   	VALUES (2,1002);
    """ ,
    """
   	INSERT INTO StudentEvent
   	VALUES (3,1003);
    """ ,
    """
   	INSERT INTO StudentEvent
   	VALUES (4,1004);
    """ ,
    """
   	INSERT INTO StudentEvent
   	VALUES (5,1005);
    """,
    """
   	INSERT INTO StudentMajor
   	VALUES (1,"APY");
    """,
    """
   	INSERT INTO StudentMajor
   	VALUES (2,"PHI");
    """,
    """
   	INSERT INTO StudentMajor
   	VALUES (3,"BIO");
    """,
    """
   	INSERT INTO StudentMajor
   	VALUES (4,"CNS");
    """,
    """
   	INSERT INTO StudentMajor
   	VALUES (5,"TPG");
    """
)

for i in inserts:
	cursor.execute(i)

# Select data
queries =(
	"""
    SELECT d.deptID, e.eventID, e.eventName, e.sDate, e.fDate
    FROM Department t, DeptEvent d, Event e
    WHERE (e.eventID = d.eventID) AND (d.deptID = t.deptID) 
    	AND (t.deptName LIKE 'Department of Biology');
    """
    ,	
	"""
    SELECT s.sID, s.sName
    FROM StudentMajor sm, Student s
    WHERE (sm.sID = s.sID) AND (sm.majorID LIKE 'CNS')
    """,
    """
    SELECT *
    FROM Department
    """,
    """
    SELECT s.sName
    FROM Department d, DeptMajor dm, Student s, StudentMajor sm
    WHERE (s.sID = sm.sID) AND (sm.majorID = dm.majorID) AND (d.deptID = dm.deptID) AND (d.numFaculty > 20)
    """,	
    """
    SELECT e.eventName, e.sDate
    FROM Event e, StudentMajor sm, StudentEvent se
    WHERE (e.eventID = se.eventID) AND (se.sID = sm.sID) AND (sm.majorID IN ('APY','CNS','PHI'))
    """
    
)


contents = (
	"""
	SELECT *
	FROM Department
	""",
	"""
	SELECT *
	FROM Major
	""",
	"""
	SELECT *
	FROM Student
	""",
	"""
	SELECT *
	FROM Event
	""",
	"""
	SELECT *
	FROM DeptMajor
	""",
	"""
	SELECT *
	FROM StudentEvent
	""",
	"""
	SELECT *
	FROM StudentMajor
	""",
	"""
	SELECT *
	FROM DeptEvent
	"""
)


# Extract column names from cursor
for q in queries:
	cursor.execute(q)

	column_names = [row[0] for row in cursor.description]

	# Fetch data and load into a pandas dataframe
	table_data = cursor.fetchall()
	df = pd.DataFrame(table_data, columns=column_names)

	# Examine dataframe
	print()
	print(df)
	print(df.columns)
	
"""	
for c in contents:
	cursor.execute(c)

	column_names = [row[0] for row in cursor.description]

	# Fetch data and load into a pandas dataframe
	table_data = cursor.fetchall()
	df = pd.DataFrame(table_data, columns=column_names)

	# Examine dataframe
	print()
	print(df)
	print(df.columns)
"""	

print()
# Example to extract a specific column
# print(df['name'])

# Commit any changes to the database
db_connect.commit()

# Close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
db_connect.close()
