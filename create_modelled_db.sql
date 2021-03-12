CREATE TABLE IF NOT EXISTS indicator_data (
	id integer NOT NULL,
	indicator_code varchar(100) NOT NULL,
	area_code varchar(100) NOT NULL,
	year integer,
	value varchar(200),
	numeric_value float(5),
	low float(5),
	high float(5),
	PRIMARY KEY (id)
);	
  CREATE TABLE IF NOT EXISTS comments (
	id integer NOT NULL,
	comments varchar(500) NOT NULL,
	CONSTRAINT fk_comments
		FOREIGN KEY(id)
			REFERENCES indicator_data(id)
);
CREATE TABLE IF NOT EXISTS grain_to_datasource (
	matching_id integer NOT NULL,
	datasource_code varchar(50) NOT NULL,
	indicator_code varchar(50) NOT NULL,
	area_code varchar(100) NOT NULL,
	year integer,
	PRIMARY KEY (matching_id),
	CONSTRAINT fk_grain_matching
		FOREIGN KEY(indicator_code, area_code, year)
			REFERENCES indicator_data(indicator_code, area_code, year)
);
CREATE TABLE IF NOT EXISTS datasources (
	datasource_code varchar(50) NOT NULL,
	display varchar(200),
	source_url varchar(200),
	description varchar(500),
	PRIMARY KEY (datasource_code),
	CONSTRAINT fk_datasource_grain
		FOREIGN KEY(datasource_code)
			REFERENCES grain_to_datasource(datasource_code)
);
CREATE TABLE IF NOT EXISTS areas (
	area_code varchar(50) NOT NULL,
	area varchar(100),
	dimension varchar(100),
	parent_code varchar(50),
	PRIMARY KEY (area_code),
	CONSTRAINT fk_area
		FOREIGN KEY (area_code)
			REFERENCES indicator_data(area_code)
);
CREATE TABLE IF NOT EXISTS indicator_info (
	indicator_code varchar(50) NOT NULL,
	indicator_name varchar(200),
	category_id int,
	url varchar(200),
	definition_xml varchar(200),
	PRIMARY KEY (indicator_code),
	CONSTRAINT fk_indicator_info
		FOREIGN KEY (indicator_code)
			REFERENCES indicator_data(indicator_code)
);
CREATE TABLE IF NOT EXISTS categories (
	category_id int NOT NULL,
	category_name varchar(200) NOT NULL,
	PRIMARY KEY(category_id),
	CONSTRAINT fk_categories
		FOREIGN KEY (category_id)
			REFERENCES indicator_info(category_id)
);
