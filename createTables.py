import parseToSqlScripts
import parser
import utils
import pymysql
from constants import *

def createCategoryTable(categoryId, text, ignoreAttrs, serviceAttrPrefix='service_'):
    positions, dictOfAttributes = parser.getPositionsByCategoryId(categoryId, text)

    checkSuccess = True

    attrList = [
            'PositionID', 
            'CategoryName', 
            'ProductPositionName', 
            'ProductPositionShortName'
        ]

    attributesIds = []
    serviceAttrs = []
    for item in dictOfAttributes:
        if dictOfAttributes[item] in ignoreAttrs:
            print('>>>>>>>>>>>>>>>> service attrs extended')
            serviceAttrs.append(serviceAttrPrefix + item)
        else:
            attributesIds.append(item)

    convertedCategoryId = utils.convertString(categoryId)
    #tableAttributesList = 'attrs_list'
    tableName = 'positions_with_category_' + convertedCategoryId
    con = pymysql.connect(HOST_NAME, USER_NAME, USER_PASSWORD, DB_NAME)
    with con:
        cur = con.cursor()
        try:
            cur.execute('DROP TABLE IF EXISTS ' + tableName)
            cur.execute(parseToSqlScripts.parseCreateTable(tableName, attrList + serviceAttrs + attributesIds))
        except Exception as e:
            print('CREATE ' + tableName + ' ' + str(e))
            checkSuccess = False
        resAttributes = []
        for key in positions:
            value = positions[key]
            attributes = [
                key,
                value['categoryName'],
                value['name'],
                value['shortName']
            ]
            for attrId in serviceAttrs:
                attributes.append(value['attributes'][attrId[len(serviceAttrPrefix):]])
            for attrId in attributesIds:
                attributes.append(value['attributes'][attrId])
            resAttributes.append(attributes)
        try:
            cur.execute(parseToSqlScripts.parseMultipleInsert(tableName, resAttributes, finishWith=';'))
        except Exception as e:
            print('INSERT INTO ' + tableName + ' table failed: ' + str(e))
            checkSuccess = False
        viewName = 'view_' + convertedCategoryId
        newTableName = 'positions_with_category_ext_' + convertedCategoryId
        res, newCols = parseToSqlScripts.createViewWithoutWindowFunc(
            viewName,
            tableName,
            attrList + serviceAttrs,
            attributesIds,
            len(resAttributes),
            finishWith=';'
        )
        try:
            cur.execute(res)
            cur.execute(parseToSqlScripts.createNewTableWithEvaluation(
                newTableName,
                viewName,
                attrList + serviceAttrs + attributesIds,
                'sqrt(' + ' + '.join(list(map(lambda x: '1 / ' + x, newCols))) + ')'
            ))
        except Exception as e:
            print('EXT ' + tableName + ' table failed: ' + str(e))
            checkSuccess = False

        try:
            cur.execute('DROP TABLE IF EXISTS ' + tableName)
            cur.execute('DROP TABLE IF EXISTS ' + viewName)
        except:
            print('DROP FAILED: skip')
            checkSuccess = False

    con.close()
    return dictOfAttributes, checkSuccess



inputFile = open('data/dataset.tsv', 'r')
text = inputFile.read().split('\n')
inputFile = open('data/ignoreAttrs.txt', 'r')
ignoreAttrs = eval(inputFile.read())
categoriesIds = parser.getUniqueCategoryIds(text)
resultQuery = ''

length = len(categoriesIds)
i = 1
highLim = int(input('TABLES NUM >> '))

#tableCategoriesList = 'categories_list'

resDict = {}
categories = []
for categoryId in categoriesIds:
    print(str(i) + ' / ' + str(highLim))
    i += 1
    categories.append(utils.convertString(categoryId))
    curDict, code = createCategoryTable(categoryId, text, ignoreAttrs)
    if code:
        resDict.update(curDict)
    if  i > highLim:
        break

outputFileForAttrs = open('data/dictOfAttributesFile.txt', 'w')
outputFileForAttrs.write(str(resDict))

outputFileForCategories = open('data/listOfCategoriesFile.txt', 'w')
outputFileForCategories.write(str(categories))