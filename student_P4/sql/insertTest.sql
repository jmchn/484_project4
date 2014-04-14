
-- create relations
CREATE TABLE soaps(soapid integer, name char(32), 
                   network char(4), rating double);
CREATE TABLE stars(starid integer, real_name char(20), 
                   plays char(12), soapid integer);


-- insert with attributes in order:
INSERT INTO stars(starid, real_name, plays) 
	VALUES (100, 'Posey, Parker', 'Tess');
SELECT stars.starid, soaps.soapid FROM stars,soaps;
