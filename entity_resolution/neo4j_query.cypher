LOAD CSV WITH HEADERS FROM "file:///ds1.csv" AS row
CREATE (p:Person) set p.source = "ds1", p += properties(row) ;

LOAD CSV WITH HEADERS FROM "file:///ds2.csv" AS row
CREATE (p:Person) set p.source = "ds2", p += properties(row) ;

LOAD CSV WITH HEADERS FROM "file:///ds3.csv" AS row
CREATE (p:Person) set p.source = "ds3", p += properties(row) ;

MATCH (p:Person) WHERE p.source = "ds1" SET p.m_yob = toInteger(p.yob) ;

MATCH (p:Person)
WHERE p.source = "ds2" SET p.m_yob = date().year - toInteger(p.age) ;

MATCH (p:Person) WHERE p.source = "ds3" SET p.m_yob = date(apoc.date.convertFormat(p.dob, "M/d/yyyy", "yyyy-MM-dd")).year ;

MATCH (p:Person)
WHERE p.source = "ds1" SET p.m_fullname = toLower(trim(p.full_name)) ;

MATCH (p:Person) WHERE p.source = "ds2"
WITH p, split(p.name,",") AS parts
SET p.m_fullname = toLower(trim(parts[1]) + ' ' + trim(parts[0]));

MATCH (p:Person) WHERE p.source = "ds3"
SET p.m_fullname = toLower(trim(p.first_name) + ' ' + trim(p.last_name)) ;

MATCH (p1:Person), (p2:Person)
WHERE p1.source <> p2.source
AND (p1.ssn = p2.ssn OR p1.passport_no = p2.passport_no)
AND id(p1) > id(p2)
CREATE (p1)-[:SAME_AS { ssn_match : p1.ssn = p2.ssn,
passport_match : p1.passport_no =
p2.passport_no}]->(p2)

MATCH (p1:Person), (p2:Person)
WHERE NOT (p1)-[:SAME_AS]-(p2)
AND p1.source <> p2.source
AND id(p1) > id(p2)
AND apoc.text.jaroWinklerDistance(p1.m_fullname, p2.m_fullname) < 0.2
CREATE (p1)-[:SIMILAR { sim_score : 1 -
apoc.text.jaroWinklerDistance(p1.m_fullname, p2.m_fullname)}]->(p2)

MATCH (p1:Person)-[sim:SIMILAR]->(p2:Person)
WHERE p1.ssn <> p2.ssn OR p1.passport_no <> p2.passport_no
DELETE sim

:param yob_threshold => 0

MATCH (p1:Person)-[sim:SIMILAR]->(p2:Person)
WITH sim, abs(p1.m_yob - p2.m_yob) AS yob_diff
SET sim.sim_score = sim.sim_score * CASE WHEN yob_diff > $yob_threshold
THEN .9 ELSE 1.1 END

:param sim_score_threshold => 0.9

MATCH (p1:Person)-[sim:SIMILAR]->(p2:Person)
WHERE sim.sim_score < $sim_score_threshold
DELETE sim

CALL gds.graph.project(
'identity-wcc',
'Person',
['SAME_AS','SIMILAR']
)

CALL gds.wcc.stream('identity-wcc')
YIELD nodeId, componentId
WITH gds.util.asNode(nodeId) AS person, componentId AS golden_id
MERGE (pg:PersonMaster { uid: golden_id })
ON CREATE SET pg.fullname = person.m_fullname,
pg.ssn = person.ssn, pg.passport_no = person.passport_no
ON MATCH SET pg.ssn = coalesce(pg.ssn,person.ssn),
pg.passport_no = coalesce(pg.passport_no,person.passport_no)
MERGE (pg)-[:HAS_REFERENCE]->(person)

MATCH (p:PersonMaster)-[:HAS_REFERENCE]->(ref)
WHERE p.passport_no = 'A465901'
WITH p, collect( { source: ref.source , details : properties(ref)}) AS refs
RETURN { master_entity_id : p.uid, references: refs }
