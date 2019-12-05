def parseValues (valuesList):
	newValuesList = []
	for each in valuesList:
		if each == None:
			newValuesList.append('NULL')
		else:
			newValuesList.append('\'' + each + '\'')
	return '(' + ', '.join(newValuesList) + ')'

def parseInsert(tableName,valuesList,finishWith=''):
	return 'INSERT INTO ' + tableName + ' VALUES ' + parseValues(valuesList) +';'

def parseMultipleInsert(tableName,valuesListList,finishWith=''):
	return 'INSERT INTO ' + tableName + ' VALUES ' + ', '.join(list(map(parseValues, valuesListList))) + finishWith
	
def parseCreateTable (tableName,attrList,finishWith=''):
	attrList = list(map(lambda x: '`' + x + "`", attrList))
	return 'CREATE TABLE ' + tableName + ' (' + ' TEXT, '.join(attrList) + ' TEXT ' * (1 if len(attrList) > 0 else 0) + ')' + finishWith

def createViewWithColFrequencyEval (viewName, tableFrom, serviceCols, attrCols, rowsCount,finishWith = ''):
	res = 'CREATE TABLE ' + viewName + ' AS \nSELECT\n'
	for i in range(len(serviceCols)):
		res += '\t' + serviceCols[i] + ',\n'
	newCols = []
	for i in range(len(attrCols)):
		res += '\t' + attrCols[i] + ',\n'
		windowFunc, newColName = parseToWindowFuncCountAutoAlias([attrCols[i]])
		newCols.append(newColName)
		res += '\t' + buildCase([('null(' + attrCols[i] + ')', str(rowsCount))],elseDo=windowFunc,alias=newColName) + ',' * (0 if i == len(attrCols)-1 else 1) + '\n'
	res += 'FROM ' + tableFrom + finishWith
	return (res,newCols)

def createNewTable (newTableName, selectFrom, colsExprNameTuples, finishWith=''):
	return 'CREATE TABLE ' + newTableName + ' AS SELECT ' + ', '.join(list(map(lambda x: x[0] + ' as ' + x[1] if type(x) == tuple else x, colsExprNameTuples))) + ' FROM ' + selectFrom + finishWith

def createNewTableWithEvaluation (newTableName, selectFrom, cols, evalExpr, finishWith=''):
	return 'CREATE TABLE ' + newTableName + ' AS SELECT ' + ', '.join(cols) + ', (' + evalExpr + ') as evalCol FROM ' + selectFrom + finishWith

def parseToWindowFuncCountAutoAlias(attrList):
	alias = '_eval_' + '_'.join(attrList)
	return parseToWindowFuncCount(attrList, alias=alias)
	
def parseToWindowFuncCount(attrList,alias=''):
	return ('COUNT(*) OVER(PARTITION BY ' + ', '.join(attrList) + ')', alias)

def buildCase(whenThenTuplesList,elseDo='',alias=''): #whenThenTuplesList example: [('null(col1)' , '1'), ('col1 > 1', '2')]
	return '(CASE WHEN ' + ' WHEN '.join(list(map(lambda x: x[0] + ' THEN ' + x[1], whenThenTuplesList))) + ' ELSE ' * (1 if len(elseDo) > 0 else 0) + elseDo + ' END)' + ' AS ' * (1 if len(alias) > 0 else 0) + alias

def join(left, joinType, right, condition):
	return '(' + left + ' ' + joinType + ' ' + right + ' ON ' + condition + ')' 
	
def createViewWithoutWindowFunc(viewName,tableFrom,serviceCols,attrCols,rowsCount,finishWith=''):
	rowsCount = str(rowsCount)
	res = 'CREATE TABLE ' + viewName + ' AS \nSELECT\n'
	newCols = list(map(lambda x: '_eval_' + x, attrCols))
	for i in range(len(serviceCols)):
		res += '\t' + serviceCols[i] + ',\n'
	for i in range(len(attrCols)):
		res += '\t' + attrCols[i] + ',\n'
	for i in range(len(newCols)):
		res += '\t' + 'COALESCE(' + newCols[i] + ', '+ rowsCount + ') AS ' + newCols[i] + ',' * (0 if i == len(newCols) - 1 else 1) + '\n'
	res += 'FROM ' + tableFrom + ' AS t0\n'
	for i in range(len(attrCols)):
		tempTableAlias = 't' + str(i+1)
		res += '\tLEFT JOIN (SELECT ' + attrCols[i] + ' AS _attr_, COUNT(*) AS ' + newCols[i] + ' FROM ' + tableFrom + ' GROUP BY ' + attrCols[i] + ') AS ' + tempTableAlias + '\n\t\tON t0.' + attrCols[i] + ' = ' + tempTableAlias + '._attr_\n'
	return (res, newCols)

#serviceCols = ['productID', 'productCategory']
#attrCols = ['col1','col2','col3']
#sqlQuery,newCols = createViewWithoutWindowFunc ('table_view', 'original_table', serviceCols, attrCols, 15, finishWith=';')
#print(sqlQuery)
#print(newCols)