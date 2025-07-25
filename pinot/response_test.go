package pinot

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSqlSelectionQueryResponse(t *testing.T) {
	var brokerResponse BrokerResponse
	respBytes := []byte("{\"resultTable\":{\"dataSchema\":{\"columnDataTypes\":[\"INT\",\"INT\",\"INT\",\"INT\",\"INT\",\"INT\",\"INT\",\"INT\",\"INT\",\"INT\",\"STRING\",\"INT\",\"INT\",\"STRING\",\"STRING\",\"INT\",\"INT\",\"INT\",\"INT\",\"INT\",\"INT\",\"INT\",\"STRING\",\"INT\",\"INT\"],\"columnNames\":[\"AtBatting\",\"G_old\",\"baseOnBalls\",\"caughtStealing\",\"doules\",\"groundedIntoDoublePlays\",\"hits\",\"hitsByPitch\",\"homeRuns\",\"intentionalWalks\",\"league\",\"numberOfGames\",\"numberOfGamesAsBatter\",\"playerID\",\"playerName\",\"playerStint\",\"runs\",\"runsBattedIn\",\"sacrificeFlies\",\"sacrificeHits\",\"stolenBases\",\"strikeouts\",\"teamID\",\"tripples\",\"yearID\"]},\"rows\":[[0,11,0,0,0,0,0,0,0,0,\"NL\",11,11,\"aardsda01\",\"David Allan\",1,0,0,0,0,0,0,\"SFN\",0,2004],[2,45,0,0,0,0,0,0,0,0,\"NL\",45,43,\"aardsda01\",\"David Allan\",1,0,0,0,1,0,0,\"CHN\",0,2006],[0,2,0,0,0,0,0,0,0,0,\"AL\",25,2,\"aardsda01\",\"David Allan\",1,0,0,0,0,0,0,\"CHA\",0,2007],[1,5,0,0,0,0,0,0,0,0,\"AL\",47,5,\"aardsda01\",\"David Allan\",1,0,0,0,0,0,1,\"BOS\",0,2008],[0,0,0,0,0,0,0,0,0,0,\"AL\",73,3,\"aardsda01\",\"David Allan\",1,0,0,0,0,0,0,\"SEA\",0,2009],[0,0,0,0,0,0,0,0,0,0,\"AL\",53,4,\"aardsda01\",\"David Allan\",1,0,0,0,0,0,0,\"SEA\",0,2010],[0,0,0,0,0,0,0,0,0,0,\"AL\",1,0,\"aardsda01\",\"David Allan\",1,0,0,0,0,0,0,\"NYA\",0,2012],[468,122,28,2,27,13,131,3,13,0,\"NL\",122,122,\"aaronha01\",\"Henry Louis\",1,58,69,4,6,2,39,\"ML1\",6,1954],[602,153,49,1,37,20,189,3,27,5,\"NL\",153,153,\"aaronha01\",\"Henry Louis\",1,105,106,4,7,3,61,\"ML1\",9,1955],[609,153,37,4,34,21,200,2,26,6,\"NL\",153,153,\"aaronha01\",\"Henry Louis\",1,106,92,7,5,2,54,\"ML1\",14,1956]]},\"exceptions\":[],\"numServersQueried\":1,\"numServersResponded\":1,\"numSegmentsQueried\":1,\"numSegmentsProcessed\":1,\"numSegmentsMatched\":1,\"numConsumingSegmentsQueried\":0,\"numDocsScanned\":10,\"numEntriesScannedInFilter\":0,\"numEntriesScannedPostFilter\":250,\"numGroupsLimitReached\":false,\"totalDocs\":97889,\"timeUsedMs\":6,\"segmentStatistics\":[],\"traceInfo\":{},\"minConsumingFreshnessTimeMs\":0}")
	err := decodeJSONWithNumber(respBytes, &brokerResponse)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(brokerResponse.AggregationResults))
	assert.Equal(t, 0, len(brokerResponse.Exceptions))
	assert.Equal(t, int64(0), brokerResponse.MinConsumingFreshnessTimeMs)
	assert.Equal(t, 0, brokerResponse.NumConsumingSegmentsQueried)
	assert.Equal(t, int64(10), brokerResponse.NumDocsScanned)
	assert.Equal(t, int64(0), brokerResponse.NumEntriesScannedInFilter)
	assert.Equal(t, int64(250), brokerResponse.NumEntriesScannedPostFilter)
	assert.False(t, brokerResponse.NumGroupsLimitReached)
	assert.Equal(t, 1, brokerResponse.NumSegmentsMatched)
	assert.Equal(t, 1, brokerResponse.NumSegmentsProcessed)
	assert.Equal(t, 1, brokerResponse.NumSegmentsQueried)
	assert.Equal(t, 1, brokerResponse.NumServersQueried)
	assert.Equal(t, 1, brokerResponse.NumServersResponded)
	assert.NotNil(t, brokerResponse.ResultTable)
	assert.Nil(t, brokerResponse.SelectionResults)
	assert.Equal(t, 6, brokerResponse.TimeUsedMs)
	assert.Equal(t, int64(97889), brokerResponse.TotalDocs)
	assert.Equal(t, 0, len(brokerResponse.TraceInfo))

	// Examine ResultTable
	assert.Equal(t, 10, brokerResponse.ResultTable.GetRowCount())
	assert.Equal(t, 25, brokerResponse.ResultTable.GetColumnCount())
	expectedColumnNames := []string{"AtBatting", "G_old", "baseOnBalls", "caughtStealing", "doules", "groundedIntoDoublePlays", "hits", "hitsByPitch", "homeRuns", "intentionalWalks", "league", "numberOfGames", "numberOfGamesAsBatter", "playerID", "playerName", "playerStint", "runs", "runsBattedIn", "sacrificeFlies", "sacrificeHits", "stolenBases", "strikeouts", "teamID", "tripples", "yearID"}
	expectedColumnTypes := []string{"INT", "INT", "INT", "INT", "INT", "INT", "INT", "INT", "INT", "INT", "STRING", "INT", "INT", "STRING", "STRING", "INT", "INT", "INT", "INT", "INT", "INT", "INT", "STRING", "INT", "INT"}
	for i := 0; i < 25; i++ {
		assert.Equal(t, expectedColumnNames[i], brokerResponse.ResultTable.GetColumnName(i))
		assert.Equal(t, expectedColumnTypes[i], brokerResponse.ResultTable.GetColumnDataType(i))
	}
}

func TestSqlAggregationQueryResponse(t *testing.T) {
	var brokerResponse BrokerResponse
	respBytes := []byte("{\"resultTable\":{\"dataSchema\":{\"columnDataTypes\":[\"LONG\"],\"columnNames\":[\"cnt\"]},\"rows\":[[97889]]},\"exceptions\":[],\"numServersQueried\":1,\"numServersResponded\":1,\"numSegmentsQueried\":1,\"numSegmentsProcessed\":1,\"numSegmentsMatched\":1,\"numConsumingSegmentsQueried\":0,\"numDocsScanned\":97889,\"numEntriesScannedInFilter\":0,\"numEntriesScannedPostFilter\":0,\"numGroupsLimitReached\":false,\"totalDocs\":97889,\"timeUsedMs\":5,\"segmentStatistics\":[],\"traceInfo\":{},\"minConsumingFreshnessTimeMs\":0}")
	err := decodeJSONWithNumber(respBytes, &brokerResponse)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(brokerResponse.AggregationResults))
	assert.Equal(t, 0, len(brokerResponse.Exceptions))
	assert.Equal(t, int64(0), brokerResponse.MinConsumingFreshnessTimeMs)
	assert.Equal(t, 0, brokerResponse.NumConsumingSegmentsQueried)
	assert.Equal(t, int64(97889), brokerResponse.NumDocsScanned)
	assert.Equal(t, int64(0), brokerResponse.NumEntriesScannedInFilter)
	assert.Equal(t, int64(0), brokerResponse.NumEntriesScannedPostFilter)
	assert.False(t, brokerResponse.NumGroupsLimitReached)
	assert.Equal(t, 1, brokerResponse.NumSegmentsMatched)
	assert.Equal(t, 1, brokerResponse.NumSegmentsProcessed)
	assert.Equal(t, 1, brokerResponse.NumSegmentsQueried)
	assert.Equal(t, 1, brokerResponse.NumServersQueried)
	assert.Equal(t, 1, brokerResponse.NumServersResponded)
	assert.NotNil(t, brokerResponse.ResultTable)
	assert.Nil(t, brokerResponse.SelectionResults)
	assert.Equal(t, 5, brokerResponse.TimeUsedMs)
	assert.Equal(t, int64(97889), brokerResponse.TotalDocs)
	assert.Equal(t, 0, len(brokerResponse.TraceInfo))
	// Examine ResultTable
	assert.Equal(t, 1, brokerResponse.ResultTable.GetRowCount())
	assert.Equal(t, 1, brokerResponse.ResultTable.GetColumnCount())
	assert.Equal(t, "cnt", brokerResponse.ResultTable.GetColumnName(0))
	assert.Equal(t, "LONG", brokerResponse.ResultTable.GetColumnDataType(0))
	assert.Equal(t, json.Number("97889"), brokerResponse.ResultTable.Get(0, 0))
	assert.Equal(t, int32(97889), brokerResponse.ResultTable.GetInt(0, 0))
	assert.Equal(t, int64(97889), brokerResponse.ResultTable.GetLong(0, 0))
	assert.Equal(t, float32(97889), brokerResponse.ResultTable.GetFloat(0, 0))
	assert.Equal(t, float64(97889), brokerResponse.ResultTable.GetDouble(0, 0))
}

func TestSqlAggregationGroupByResponse(t *testing.T) {
	var brokerResponse BrokerResponse
	respBytes := []byte("{\"resultTable\":{\"dataSchema\":{\"columnDataTypes\":[\"STRING\",\"LONG\",\"DOUBLE\"],\"columnNames\":[\"teamID\",\"cnt\",\"sum_homeRuns\"]},\"rows\":[[\"ANA\",337,1324.0],[\"BL2\",197,136.0],[\"ARI\",727,2715.0],[\"BL1\",48,24.0],[\"ALT\",17,2.0],[\"ATL\",1951,7312.0],[\"BFN\",122,105.0],[\"BL3\",36,32.0],[\"BFP\",26,20.0],[\"BAL\",2380,9164.0]]},\"exceptions\":[],\"numServersQueried\":1,\"numServersResponded\":1,\"numSegmentsQueried\":1,\"numSegmentsProcessed\":1,\"numSegmentsMatched\":1,\"numConsumingSegmentsQueried\":0,\"numDocsScanned\":97889,\"numEntriesScannedInFilter\":0,\"numEntriesScannedPostFilter\":195778,\"numGroupsLimitReached\":true,\"totalDocs\":97889,\"timeUsedMs\":24,\"segmentStatistics\":[],\"traceInfo\":{},\"minConsumingFreshnessTimeMs\":0}")
	err := decodeJSONWithNumber(respBytes, &brokerResponse)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(brokerResponse.AggregationResults))
	assert.Equal(t, 0, len(brokerResponse.Exceptions))
	assert.Equal(t, int64(0), brokerResponse.MinConsumingFreshnessTimeMs)
	assert.Equal(t, 0, brokerResponse.NumConsumingSegmentsQueried)
	assert.Equal(t, int64(97889), brokerResponse.NumDocsScanned)
	assert.Equal(t, int64(0), brokerResponse.NumEntriesScannedInFilter)
	assert.Equal(t, int64(195778), brokerResponse.NumEntriesScannedPostFilter)
	assert.True(t, brokerResponse.NumGroupsLimitReached)
	assert.Equal(t, 1, brokerResponse.NumSegmentsMatched)
	assert.Equal(t, 1, brokerResponse.NumSegmentsProcessed)
	assert.Equal(t, 1, brokerResponse.NumSegmentsQueried)
	assert.Equal(t, 1, brokerResponse.NumServersQueried)
	assert.Equal(t, 1, brokerResponse.NumServersResponded)
	assert.NotNil(t, brokerResponse.ResultTable)
	assert.Nil(t, brokerResponse.SelectionResults)
	assert.Equal(t, 24, brokerResponse.TimeUsedMs)
	assert.Equal(t, int64(97889), brokerResponse.TotalDocs)
	assert.Equal(t, 0, len(brokerResponse.TraceInfo))
	// Examine ResultTable
	assert.Equal(t, 10, brokerResponse.ResultTable.GetRowCount())
	assert.Equal(t, 3, brokerResponse.ResultTable.GetColumnCount())
	assert.Equal(t, "teamID", brokerResponse.ResultTable.GetColumnName(0))
	assert.Equal(t, "STRING", brokerResponse.ResultTable.GetColumnDataType(0))
	assert.Equal(t, "cnt", brokerResponse.ResultTable.GetColumnName(1))
	assert.Equal(t, "LONG", brokerResponse.ResultTable.GetColumnDataType(1))
	assert.Equal(t, "sum_homeRuns", brokerResponse.ResultTable.GetColumnName(2))
	assert.Equal(t, "DOUBLE", brokerResponse.ResultTable.GetColumnDataType(2))

	assert.Equal(t, "ANA", brokerResponse.ResultTable.GetString(0, 0))
	assert.Equal(t, int64(337), brokerResponse.ResultTable.GetLong(0, 1))
	assert.Equal(t, float64(1324.0), brokerResponse.ResultTable.GetDouble(0, 2))

	assert.Equal(t, "BL2", brokerResponse.ResultTable.GetString(1, 0))
	assert.Equal(t, int64(197), brokerResponse.ResultTable.GetLong(1, 1))
	assert.Equal(t, float64(136.0), brokerResponse.ResultTable.GetDouble(1, 2))
}

func TestWrongTypeResponse(t *testing.T) {
	var brokerResponse BrokerResponse
	respBytes := []byte("{\"resultTable\":{\"dataSchema\":{\"columnDataTypes\":[\"STRING\",\"LONG\",\"DOUBLE\"],\"columnNames\":[\"teamID\",\"cnt\",\"sum_homeRuns\"]},\"rows\":[[\"ANA\",9223372036854775808, 1e309]]},\"exceptions\":[],\"numServersQueried\":1,\"numServersResponded\":1,\"numSegmentsQueried\":1,\"numSegmentsProcessed\":1,\"numSegmentsMatched\":1,\"numConsumingSegmentsQueried\":0,\"numDocsScanned\":97889,\"numEntriesScannedInFilter\":0,\"numEntriesScannedPostFilter\":195778,\"numGroupsLimitReached\":true,\"totalDocs\":97889,\"timeUsedMs\":24,\"segmentStatistics\":[],\"traceInfo\":{},\"minConsumingFreshnessTimeMs\":0}")
	err := decodeJSONWithNumber(respBytes, &brokerResponse)
	assert.Nil(t, err)
	assert.Equal(t, "ANA", brokerResponse.ResultTable.GetString(0, 0))
	// Assert wrong type
	assert.Equal(t, int32(0), brokerResponse.ResultTable.GetInt(0, 1))
	assert.Equal(t, int64(0), brokerResponse.ResultTable.GetLong(0, 1))
	assert.Equal(t, float32(0), brokerResponse.ResultTable.GetFloat(0, 2))
	assert.Equal(t, float64(0), brokerResponse.ResultTable.GetDouble(0, 2))
}

func TestExceptionResponse(t *testing.T) {
	var brokerResponse BrokerResponse
	respBytes := []byte("{\"resultTable\":{\"dataSchema\":{\"columnDataTypes\":[\"DOUBLE\"],\"columnNames\":[\"max(league)\"]},\"rows\":[]},\"exceptions\":[{\"errorCode\":200,\"message\":\"QueryExecutionError:\\njava.lang.NumberFormatException: For input string: \\\"UA\\\"\\n\\tat sun.misc.FloatingDecimal.readJavaFormatString(FloatingDecimal.java:2043)\\n\\tat sun.misc.FloatingDecimal.parseDouble(FloatingDecimal.java:110)\\n\\tat java.lang.Double.parseDouble(Double.java:538)\\n\\tat org.apache.pinot.core.segment.index.readers.StringDictionary.getDoubleValue(StringDictionary.java:58)\\n\\tat org.apache.pinot.core.operator.query.DictionaryBasedAggregationOperator.getNextBlock(DictionaryBasedAggregationOperator.java:81)\\n\\tat org.apache.pinot.core.operator.query.DictionaryBasedAggregationOperator.getNextBlock(DictionaryBasedAggregationOperator.java:47)\\n\\tat org.apache.pinot.core.operator.BaseOperator.nextBlock(BaseOperator.java:48)\\n\\tat org.apache.pinot.core.operator.CombineOperator$1.runJob(CombineOperator.java:102)\\n\\tat org.apache.pinot.core.util.trace.TraceRunnable.run(TraceRunnable.java:40)\\n\\tat java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)\\n\\tat java.util.concurrent.FutureTask.run(FutureTask.java:266)\\n\\tat java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)\\n\\tat shaded.com.google.common.util.concurrent.TrustedListenableFutureTask$TrustedFutureInterruptibleTask.runInterruptibly(TrustedListenableFutureTask.java:111)\\n\\tat shaded.com.google.common.util.concurrent.InterruptibleTask.run(InterruptibleTask.java:58)\"}],\"numServersQueried\":1,\"numServersResponded\":1,\"numSegmentsQueried\":1,\"numSegmentsProcessed\":0,\"numSegmentsMatched\":0,\"numConsumingSegmentsQueried\":0,\"numDocsScanned\":0,\"numEntriesScannedInFilter\":0,\"numEntriesScannedPostFilter\":0,\"numGroupsLimitReached\":false,\"totalDocs\":97889,\"timeUsedMs\":5,\"segmentStatistics\":[],\"traceInfo\":{},\"minConsumingFreshnessTimeMs\":0}")
	err := decodeJSONWithNumber(respBytes, &brokerResponse)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(brokerResponse.AggregationResults))
	assert.Equal(t, 1, len(brokerResponse.Exceptions))
	assert.Equal(t, int64(0), brokerResponse.MinConsumingFreshnessTimeMs)
	assert.Equal(t, 0, brokerResponse.NumConsumingSegmentsQueried)
	assert.Equal(t, int64(0), brokerResponse.NumDocsScanned)
	assert.Equal(t, int64(0), brokerResponse.NumEntriesScannedInFilter)
	assert.Equal(t, int64(0), brokerResponse.NumEntriesScannedPostFilter)
	assert.False(t, brokerResponse.NumGroupsLimitReached)
	assert.Equal(t, 0, brokerResponse.NumSegmentsMatched)
	assert.Equal(t, 0, brokerResponse.NumSegmentsProcessed)
	assert.Equal(t, 1, brokerResponse.NumSegmentsQueried)
	assert.Equal(t, 1, brokerResponse.NumServersQueried)
	assert.Equal(t, 1, brokerResponse.NumServersResponded)
	assert.NotNil(t, brokerResponse.ResultTable)
	assert.Nil(t, brokerResponse.SelectionResults)
	assert.Equal(t, 5, brokerResponse.TimeUsedMs)
	assert.Equal(t, int64(97889), brokerResponse.TotalDocs)
	assert.Equal(t, 0, len(brokerResponse.TraceInfo))
	// Examine ResultTable
	assert.Equal(t, 0, brokerResponse.ResultTable.GetRowCount())
	assert.Equal(t, 1, brokerResponse.ResultTable.GetColumnCount())
	assert.Equal(t, "max(league)", brokerResponse.ResultTable.GetColumnName(0))
	assert.Equal(t, "DOUBLE", brokerResponse.ResultTable.GetColumnDataType(0))
	assert.Equal(t, 200, brokerResponse.Exceptions[0].ErrorCode)
	assert.True(t, strings.Contains(brokerResponse.Exceptions[0].Message, "QueryExecutionError:"))
}

func TestResultTableGetMethods(t *testing.T) {
	// Test various edge cases and type conversions for Get* methods
	resultTable := ResultTable{
		DataSchema: RespSchema{
			ColumnDataTypes: []string{"INT", "LONG", "FLOAT", "DOUBLE", "STRING", "INT", "LONG", "FLOAT", "DOUBLE", "STRING"},
			ColumnNames:     []string{"int_val", "long_val", "float_val", "double_val", "string_val", "decimal_int", "decimal_long", "large_float", "large_double", "non_number"},
		},
		Rows: [][]interface{}{
			{
				json.Number("123"),                     // int_val: regular integer
				json.Number("456789"),                  // long_val: regular long
				json.Number("123.45"),                  // float_val: regular float
				json.Number("789.123"),                 // double_val: regular double
				json.Number("999"),                     // string_val: number as string
				json.Number("42.0"),                    // decimal_int: decimal that should convert to int
				json.Number("12345.0"),                 // decimal_long: decimal that should convert to long
				json.Number("999999999999.0"),          // large_float: large number for float conversion
				json.Number("1.7976931348623157e+308"), // large_double: very large double
				"not_a_number",                         // non_number: string that's not a json.Number
			},
			{
				json.Number("9223372036854775808"),  // out of range long (max_int64 + 1)
				json.Number("-9223372036854775809"), // out of range long (min_int64 - 1)
				json.Number("1e309"),                // out of range float (infinity)
				json.Number("-1e309"),               // out of range double (negative infinity)
				"string_value",                      // actual string
				json.Number("42.5"),                 // non-whole number for int conversion
				json.Number("123.7"),                // non-whole number for long conversion
				json.Number("3.4028235e+39"),        // float32 max + 1 (out of range)
				json.Number("inf"),                  // infinity string
				123,                                 // non-json.Number integer
			},
		},
	}

	// Test row 0 - normal cases
	assert.Equal(t, int32(123), resultTable.GetInt(0, 0))
	assert.Equal(t, int64(456789), resultTable.GetLong(0, 1))
	assert.Equal(t, float32(123.45), resultTable.GetFloat(0, 2))
	assert.Equal(t, float64(789.123), resultTable.GetDouble(0, 3))
	assert.Equal(t, "999", resultTable.GetString(0, 4))

	// Test decimal to integer conversions
	assert.Equal(t, int32(42), resultTable.GetInt(0, 5))     // 42.0 -> 42
	assert.Equal(t, int64(12345), resultTable.GetLong(0, 6)) // 12345.0 -> 12345

	// Test large number conversions
	assert.Equal(t, float32(999999999999.0), resultTable.GetFloat(0, 7))
	assert.Equal(t, float64(1.7976931348623157e+308), resultTable.GetDouble(0, 8))

	// Test non-json.Number types
	assert.Equal(t, "not_a_number", resultTable.GetString(0, 9))
	assert.Equal(t, int32(0), resultTable.GetInt(0, 9))      // should return 0 for non-json.Number
	assert.Equal(t, int64(0), resultTable.GetLong(0, 9))     // should return 0 for non-json.Number
	assert.Equal(t, float32(0), resultTable.GetFloat(0, 9))  // should return 0 for non-json.Number
	assert.Equal(t, float64(0), resultTable.GetDouble(0, 9)) // should return 0 for non-json.Number

	// Test row 1 - edge cases and out-of-range values
	assert.Equal(t, int64(0), resultTable.GetLong(1, 0))     // out of range long should return 0
	assert.Equal(t, int64(0), resultTable.GetLong(1, 1))     // out of range long should return 0
	assert.Equal(t, float32(0), resultTable.GetFloat(1, 2))  // out of range float should return 0
	assert.Equal(t, float64(0), resultTable.GetDouble(1, 3)) // out of range double should return 0

	assert.Equal(t, "string_value", resultTable.GetString(1, 4))

	// Test non-whole decimal to integer conversions (should return 0)
	assert.Equal(t, int32(0), resultTable.GetInt(1, 5))  // 42.5 is not whole number
	assert.Equal(t, int64(0), resultTable.GetLong(1, 6)) // 123.7 is not whole number

	// Test out of range float
	assert.Equal(t, float32(0), resultTable.GetFloat(1, 7)) // out of range float32

	// Test infinity double conversion
	assert.Equal(t, float64(0), resultTable.GetDouble(1, 8)) // infinity should return 0

	// Test non-json.Number integer
	assert.Equal(t, "123", resultTable.GetString(1, 9))
	assert.Equal(t, int32(0), resultTable.GetInt(1, 9))      // should return 0 for non-json.Number
	assert.Equal(t, int64(0), resultTable.GetLong(1, 9))     // should return 0 for non-json.Number
	assert.Equal(t, float32(0), resultTable.GetFloat(1, 9))  // should return 0 for non-json.Number
	assert.Equal(t, float64(0), resultTable.GetDouble(1, 9)) // should return 0 for non-json.Number
}

func TestResultTableGetMethodsWithInvalidInput(t *testing.T) {
	// Test with malformed JSON numbers and invalid inputs
	resultTable := ResultTable{
		DataSchema: RespSchema{
			ColumnDataTypes: []string{"INT", "STRING", "DOUBLE"},
			ColumnNames:     []string{"invalid_json", "string_val", "valid_double"},
		},
		Rows: [][]interface{}{
			{
				json.Number("invalid_number_format"), // malformed number
				"test_string",                        // regular string
				json.Number("123.456"),               // valid double
			},
		},
	}

	// Test malformed json.Number - should return 0 for numeric conversions
	assert.Equal(t, int32(0), resultTable.GetInt(0, 0))
	assert.Equal(t, int64(0), resultTable.GetLong(0, 0))
	assert.Equal(t, float32(0), resultTable.GetFloat(0, 0))
	assert.Equal(t, float64(0), resultTable.GetDouble(0, 0))

	// Test regular string
	assert.Equal(t, "test_string", resultTable.GetString(0, 1))

	// Test valid double
	assert.Equal(t, float64(123.456), resultTable.GetDouble(0, 2))
}

func TestResultTableUtilityMethods(t *testing.T) {
	resultTable := ResultTable{
		DataSchema: RespSchema{
			ColumnDataTypes: []string{"INT", "STRING", "DOUBLE"},
			ColumnNames:     []string{"col1", "col2", "col3"},
		},
		Rows: [][]interface{}{
			{json.Number("1"), "value1", json.Number("1.1")},
			{json.Number("2"), "value2", json.Number("2.2")},
		},
	}

	// Test utility methods for better coverage
	assert.Equal(t, 2, resultTable.GetRowCount())
	assert.Equal(t, 3, resultTable.GetColumnCount())
	assert.Equal(t, "col1", resultTable.GetColumnName(0))
	assert.Equal(t, "col2", resultTable.GetColumnName(1))
	assert.Equal(t, "col3", resultTable.GetColumnName(2))
	assert.Equal(t, "INT", resultTable.GetColumnDataType(0))
	assert.Equal(t, "STRING", resultTable.GetColumnDataType(1))
	assert.Equal(t, "DOUBLE", resultTable.GetColumnDataType(2))

	// Test Get method
	assert.Equal(t, json.Number("1"), resultTable.Get(0, 0))
	assert.Equal(t, "value1", resultTable.Get(0, 1))
	assert.Equal(t, json.Number("1.1"), resultTable.Get(0, 2))
}

func TestResultTableBoundaryValues(t *testing.T) {
	// Test with boundary values for different numeric types
	resultTable := ResultTable{
		DataSchema: RespSchema{
			ColumnDataTypes: []string{"INT", "LONG", "FLOAT", "DOUBLE"},
			ColumnNames:     []string{"int_max", "long_max", "float_max", "double_max"},
		},
		Rows: [][]interface{}{
			{
				json.Number("2147483647"),              // int32 max
				json.Number("9223372036854775807"),     // int64 max
				json.Number("3.4028234e+38"),           // float32 near max (safe value)
				json.Number("1.7976931348623157e+308"), // float64 max
			},
			{
				json.Number("-2147483648"),              // int32 min
				json.Number("-9223372036854775808"),     // int64 min
				json.Number("-3.4028234e+38"),           // float32 near min (safe value)
				json.Number("-1.7976931348623157e+308"), // float64 min
			},
		},
	}

	// Test maximum values
	assert.Equal(t, int32(2147483647), resultTable.GetInt(0, 0))
	assert.Equal(t, int64(9223372036854775807), resultTable.GetLong(0, 1))
	// Note: float32 max value should be converted successfully
	result := resultTable.GetFloat(0, 2)
	assert.True(t, result != 0, "float32 max value should not be converted to 0")
	assert.Equal(t, float64(1.7976931348623157e+308), resultTable.GetDouble(0, 3))

	// Test minimum values
	assert.Equal(t, int32(-2147483648), resultTable.GetInt(1, 0))
	assert.Equal(t, int64(-9223372036854775808), resultTable.GetLong(1, 1))
	// Note: float32 min value should be converted successfully
	result2 := resultTable.GetFloat(1, 2)
	assert.True(t, result2 != 0, "float32 min value should not be converted to 0")
	assert.Equal(t, float64(-1.7976931348623157e+308), resultTable.GetDouble(1, 3))
}
