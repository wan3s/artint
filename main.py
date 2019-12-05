import pymysql
from constants import *

def getQuery(tableName, tgtCol = 'evalCol'):
	return 'SELECT * FROM ' + tableName + ' AS t1 INNER JOIN (SELECT max(' + tgtCol + ') as evalColMax FROM ' + tableName + ') AS t2 ON t1.' + tgtCol + ' = t2.evalColMax'

inputFileAttrs = open('data/dictOfAttributesFile.txt', 'r')
dictOfAttrs = eval(inputFileAttrs.read())

inputFileCategories = open('data/listOfCategoriesFile.txt', 'r')
listOfCategories = eval(inputFileCategories.read())

for category in listOfCategories:
    con = pymysql.connect(HOST_NAME, USER_NAME, USER_PASSWORD, DB_NAME, cursorclass=pymysql.cursors.DictCursor)
    cur = con.cursor()
    cur.execute(getQuery('positions_with_category_ext_' + category))
    for uniqueAnswer in cur.fetchall():
        print('В категории \"' + uniqueAnswer['CategoryName'] + '\" товар \"' + uniqueAnswer['ProductPositionName'] + '\" уникален по следующим характеристикам: ')
        for key in uniqueAnswer:
            if key[:8] != 'attr_id_' or uniqueAnswer[key] == None:
                continue
            print('\t' + dictOfAttrs[key] + ': ' + uniqueAnswer[key])
    print('=============================')

    con.close()