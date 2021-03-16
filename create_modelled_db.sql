CREATE TABLE IF NOT EXISTS values_table (
	measurement_id integer NOT NULL,
	indicator_id integer NOT NULL,
	area_id integer NOT NULL,
	measurement_year integer,
	measurement_value varchar(200),
	numeric_value float(5),
	low float(5),
	high float(5),
	is_main_measure BIT,
	PRIMARY KEY (measurement_id)
);
  CREATE TABLE IF NOT EXISTS comments (
	comment_id integer NOT NULL,
	measurement_id integer NOT NULL,
	comments varchar(500) NOT NULL,
	PRIMARY KEY (comment_id)
	CONSTRAINT fk_comments
		FOREIGN KEY(measurement_id)
			REFERENCES values_table(measurement_id)
);
CREATE TABLE IF NOT EXISTS datasource_bridge_table (
	matching_id integer NOT NULL,
	datasource_id integer NOT NULL,
	measurement_id integer NOT NULL,
	PRIMARY KEY (matching_id),
	CONSTRAINT fk_grain_matching
		FOREIGN KEY(measurement_id)
			REFERENCES values_table(measurement_id)
);
CREATE TABLE IF NOT EXISTS datasources (
	datasource_id integer NOT NULL,
	datasource_code varchar(50) NOT NULL,
	display varchar(200),
	source_url varchar(200),
	description varchar(500),
	PRIMARY KEY (datasource_id),
	CONSTRAINT fk_datasource_grain
		FOREIGN KEY(datasource_id)
			REFERENCES datasource_bridge_table(datasource_id)
);
CREATE TABLE IF NOT EXISTS areas (
	area_id integer NOT NULL,
	area_code varchar(50) NOT NULL,
	area_name varchar(100),
	dimension varchar(100),
	parent_code varchar(50),
	PRIMARY KEY (area_id),
	CONSTRAINT fk_area
		FOREIGN KEY (area_id)
			REFERENCES values_table(area_id)
);
CREATE TABLE IF NOT EXISTS indicator_info (
	indicator_id integer NOT NULL,
	indicator_code varchar(50) NOT NULL,
	indicator_name varchar(200),
	category_id int,
	url varchar(200),
	definition_xml varchar(200),
	PRIMARY KEY (indicator_id),
	CONSTRAINT fk_indicator_info
		FOREIGN KEY (indicator_id)
			REFERENCES values_table(indicator_id)
);
CREATE TABLE IF NOT EXISTS categories (
	category_id integer NOT NULL,
	category_name varchar(200) NOT NULL,
	PRIMARY KEY(category_id),
	CONSTRAINT fk_categories
		FOREIGN KEY (category_id)
			REFERENCES values_table(category_id)
);
CREATE TABLE IF NOT EXISTS results_to_parameters (
	result_to_param_id integer NOT NULL,
	measurement_id integer NOT NULL,
	parameter_id integer NOT NULL,
	parameter_value varchar(200) NOT NULL,
	PRIMARY KEY(result_to_param_id),
	CONSTRAINT fk_value_to_category
		FOREIGN KEY (measurement_id)
			REFERENCES value_table(measurement_id)
);
CREATE TABLE IF NOT EXISTS parameters (
	parameter_id integer NOT NULL,
	parameter_name varchar(200) NOT NULL,
	PRIMARY KEY(parameter_id),
	CONSTRAINT fk_parameter_bridge
		FOREIGN KEY (parameter_id)
			REFERENCES results_parameters(parameter_id)
);
