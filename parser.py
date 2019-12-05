import utils

def getPositionsByCategoryId(categoryId, text):
    positions = {}
    dictOfAttributes = {}
    for line in text:
        arr = line.split('\t')
        if len(arr) < 2 or arr[7] != categoryId:
            continue
        attrId = 'attr_id_' + utils.convertString(arr[6])
        if arr[0] not in positions:
            positions[arr[0]] = {
                'categoryName': arr[7],
                'name': arr[1],
                'shortName': arr[2],
                #'okei': arr[8],
                'attributes': {
                    attrId: arr[4] if arr[4] != 'NULL' else None
                }
            }
        else:
            positions[arr[0]]['attributes'][attrId] = arr[4] if arr[4] != 'NULL' else None
        dictOfAttributes[attrId] = arr[6]

    for key in positions:
        item = positions[key]
        for elem in dictOfAttributes:
            if elem not in item['attributes']:
                item['attributes'][elem] = None

    return positions, dictOfAttributes

def getUniqueCategoryIds(text):
    categories = set()
    for line in text:
        arr = line.split('\t')
        if len(arr) >=2:
            categories.add(arr[7])
    return categories

def getFilterByOkeiTable(text):
    resTable = ''
    for line in text:
        if line.split('\t')[8] != 'NULL':
            resTable += line + '\n'
    return resTable


