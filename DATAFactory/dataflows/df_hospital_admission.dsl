source(output(
		country as string,
		indicator as string,
		date as date,
		year_week as string,
		value as double,
		source as string,
		url as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false) ~> HospitalAdmission
source(output(
		country as string,
		country_code_2_digit as string,
		country_code_3_digit as string,
		continent as string,
		population as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false) ~> CountryCodeSource
source(output(
		date_key as string,
		date as string,
		year as string,
		month as string,
		day as string,
		day_name as string,
		day_of_year as string,
		week_of_month as string,
		week_of_year as string,
		month_name as string,
		year_month as string,
		year_week as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false) ~> DimDateSource
HospitalAdmission select(mapColumn(
		country,
		indicator,
		reported_date = date,
		reported_year_week = year_week,
		value,
		source
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> SelectRequiredFeilds
SelectRequiredFeilds, CountryCodeSource lookup(SelectRequiredFeilds@country == CountryCodeSource@country,
	multiple: false,
	pickup: 'any',
	broadcast: 'auto')~> CountryLookup
CountryLookup select(mapColumn(
		country = SelectRequiredFeilds@country,
		indicator,
		reported_date,
		reported_year_week,
		value,
		source,
		country = CountryCodeSource@country,
		country_code_2_digit,
		country_code_3_digit,
		population
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> SelectRequiredColumn2
SelectRequiredColumn2 split(indicator == "Weekly new hospital admissions per 100k" || indicator == "Weekly new ICU admissions per 100k",
	disjoint: false) ~> SplitDailyFRomWeekely@(Weekly, Daily)
DimDateSource derive(ecdc_year_week = year+"-W"+lpad(week_of_year,2,"0")) ~> EcdcYEARWeek
EcdcYEARWeek aggregate(groupBy(ecdc_year_week),
	week_start_date = min(date),
		week_end_date = max(date)) ~> AggrigateStartAndEndDate
SplitDailyFRomWeekely@Weekly, AggrigateStartAndEndDate join(reported_year_week == ecdc_year_week,
	joinType:'inner',
	matchType:'exact',
	ignoreSpaces: false,
	broadcast: 'auto')~> JoinWithDate
JoinWithDate pivot(groupBy(country,
		country_code_2_digit,
		country_code_3_digit,
		population,
		reported_year_week,
		source,
		week_start_date,
		week_end_date),
	pivotBy(indicator, ['Weekly new hospital admissions per 100k', 'Weekly new ICU admissions per 100k']),
	count = sum(value),
	columnNaming: '$V_$N',
	lateral: true) ~> PivotWeekely
SplitDailyFRomWeekely@Daily pivot(groupBy(country,
		country_code_2_digit,
		country_code_3_digit,
		population,
		reported_date,
		source),
	pivotBy(indicator, ['Daily hospital occupancy', 'Daily ICU occupancy']),
	count = sum(value),
	columnNaming: '$V_$N',
	lateral: true) ~> PivotDaily
PivotWeekely sort(desc(reported_year_week, true),
	asc(country, true),
	partitionBy('hash', 1)) ~> SortWeekely
PivotDaily sort(desc(reported_date, true),
	asc(country, true),
	partitionBy('hash', 1)) ~> SortDaily
SortWeekely select(mapColumn(
		country,
		country_code_2_digit,
		country_code_3_digit,
		population,
		reported_year_week,
		reported_week_start_date = week_start_date,
		reported_week_end_date = week_end_date,
		new_hospital_occupancy_count = {Weekly new hospital admissions per 100k_count},
		new_icu_occupancy_count = {Weekly new ICU admissions per 100k_count},
		source
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> SelectWeekely
SortDaily select(mapColumn(
		country,
		country_code_2_digit,
		country_code_3_digit,
		population,
		reported_date,
		hospital_occupancy_count = {Daily hospital occupancy_count},
		icu_occupancy_count = {Daily ICU occupancy_count},
		source
	),
	partitionBy('hash', 1),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> SelectDaily
SelectWeekely sink(allowSchemaDrift: true,
	validateSchema: false,
	truncate: true,
	umask: 0022,
	preCommands: [],
	postCommands: [],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> WeeklySink
SelectDaily sink(allowSchemaDrift: true,
	validateSchema: false,
	truncate: true,
	umask: 0022,
	preCommands: [],
	postCommands: [],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> SinkDaily