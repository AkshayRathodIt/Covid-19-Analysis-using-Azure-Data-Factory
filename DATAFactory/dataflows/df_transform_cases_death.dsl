source(output(
		country as string,
		country_code as string,
		continent as string,
		population as integer,
		indicator as string,
		daily_count as integer,
		date as date,
		rate_14_day as double,
		source as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false) ~> CasesAndDeathSource
source(output(
		country as string,
		country_code_2_digit as string,
		country_code_3_digit as string,
		continent as string,
		population as integer
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false) ~> countryLookup
CasesAndDeathSource filter(continent == "Europe" && not(isNull(country_code))) ~> FilterEuropeData
FilterEuropeData select(mapColumn(
		country,
		country_code,
		population,
		indicator,
		daily_count,
		source,
		each(match(name=="date"),
			"reported"+"_date" = $$)
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> SelectOnlyRequiredFeilds
SelectOnlyRequiredFeilds pivot(groupBy(country,
		country_code,
		population,
		source,
		reported_date),
	pivotBy(indicator, ['confirmed cases', 'deaths']),
	count = sum(daily_count),
	columnNaming: '$V_$N',
	lateral: true) ~> pivotCounts
pivotCounts, countryLookup lookup(pivotCounts@country == countryLookup@country,
	multiple: false,
	pickup: 'any',
	broadcast: 'auto')~> countryCodelookup
countryCodelookup select(mapColumn(
		country = pivotCounts@country,
		country_code_2_digit,
		country_code_3_digit,
		population = pivotCounts@population,
		cases_count = {confirmed cases_count},
		deaths_count,
		reported_date,
		source
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> selectMainColumn
selectMainColumn sink(allowSchemaDrift: true,
	validateSchema: false,
	partitionFileNames:['processed_cases_and_death.csv'],
	truncate: true,
	umask: 0022,
	preCommands: [],
	postCommands: [],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	partitionBy('hash', 1)) ~> SinkData