# neo4j_projects
<br>
LOAD CSV WITH HEADERS FROM "file:///ds1.csv" AS row<br>
CREATE (p:Person) set p.source = "ds1", p += properties(row) ;<br>
<br>
LOAD CSV WITH HEADERS FROM "file:///ds2.csv" AS row<br>
CREATE (p:Person) set p.source = "ds2", p += properties(row) ;<br>
<br>
LOAD CSV WITH HEADERS FROM "file:///ds3.csv" AS row<br>
CREATE (p:Person) set p.source = "ds3", p += properties(row) ;<br>
<br>
Initial db state after load datasets.
<br>
<img src="entity_resolution/img/initialPersonDataLeftDb.jpg" width="300" />
<img src="entity_resolution/img/initialPersonData.jpg" width="400" />
<br>
Data normalization<br>
MATCH (p:Person) WHERE p.source = "ds1" SET p.m_yob = toInteger(p.yob) ;<br>
<br>
MATCH (p:Person)<br>
WHERE p.source = "ds2" SET p.m_yob = date().year - toInteger(p.age) ;<br>
<br>
MATCH (p:Person) WHERE p.source = "ds3" SET p.m_yob = date(apoc.date.convertFormat(p.dob, "M/d/yyyy", "yyyy-MM-dd")).year ;<br>
<br>
MATCH (p:Person)<br>
WHERE p.source = "ds1" SET p.m_fullname = toLower(trim(p.full_name)) ;<br>
<br>
MATCH (p:Person) WHERE p.source = "ds2"<br>
WITH p, split(p.name,",") AS parts<br>
SET p.m_fullname = toLower(trim(parts[1]) + ' ' + trim(parts[0]));<br>
<br>
MATCH (p:Person) WHERE p.source = "ds3"<br>
SET p.m_fullname = toLower(trim(p.first_name) + ' ' + trim(p.last_name)) ;<br>
<br>
MATCH (p1:Person), (p2:Person)<br>
WHERE p1.source <> p2.source<br>
AND (p1.ssn = p2.ssn OR p1.passport_no = p2.passport_no)<br>
AND id(p1) > id(p2)<br>
CREATE (p1)-[:SAME_AS { ssn_match : p1.ssn = p2.ssn,<br>
passport_match : p1.passport_no =<br>
p2.passport_no}]->(p2)<br>
<img src="entity_resolution/img/SameAsInit.jpg" width="800" />
<br>
