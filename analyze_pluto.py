from csv import reader
from pyspark import SparkContext

boroughs = ['BK', 'MN', 'QN', 'SI']
files = ['BX13V1.csv', 'SI14v1.csv', 'QN13V1.csv', 'BX09v1.csv', 'MN13v2.csv', 'BX15v1.csv',
         'BK09v2.csv', 'MN12v1.csv', 'QN06C.csv', 'QN10V2.csv', 'BK13V1.csv', 'QN14v2.csv',
         'BK12V2.csv', 'BK13v2.csv', 'BX12V2.csv', 'SI10V2.csv', 'QN09v2.csv', 'BX09v2.csv',
         'SI09v2.csv', 'MN09v1.csv', 'BX14v1.csv', 'BK14v1.csv', 'BK12v1.csv', 'SI11v1.csv',
         'SI12V2.csv', 'MN10V1.csv', 'BK10V2.csv', 'BX10V1.csv', 'MN09v2.csv', 'BK10V1.csv',
         'BX14v2.csv', 'BX10V2.csv', 'MN14v2.csv', 'BK15v1.csv', 'MN11v1.csv', 'SI15v1.csv',
         'QN12V1.csv', 'QN12V2.csv', 'MN06C.csv', 'QN11v1.csv', 'BX12V1.csv', 'MN11V2.csv',
         'BX11V2.csv', 'SI06C.csv', 'QN09v1.csv', 'MN14v1.csv', 'BX13v2.csv', 'SI14v2.csv',
         'BK11v1.csv', 'BX06C.csv', 'SI10V1.csv', 'MN15v1.csv', 'SI13V1.csv', 'MN10V2.csv',
         'QN10V1.csv', 'QN11V2.csv', 'BX11V1.csv', 'QN13v2.csv', 'MN12V2.csv', 'QN14v1.csv',
         'MN13V1.csv', 'BK14v2.csv', 'BK06C.csv', 'SI11V2.csv', 'BK11V2.csv', 'BK09v1.csv',
         'SI13v2.csv', 'QN15v1.csv', 'SI09v1.csv', 'SI12V1.csv', 'BK07C.csv', 'BX07C.csv',
         'MN07C.csv', 'QN07C.csv', 'SI07C.csv']

# Assumes the files are in ./data/PLUTO
sc = SparkContext()

def grab_columns(x, columns):
    returnlist = []
    x = x.split(',')
    for c in columns:
        try:
            if '\0' in x[c]:
                returnlist.append('NULL')
            else:
                returnlist.append(int(x[c]))
        except:
            returnlist.append('NULL')
    return returnlist

def process_file(filename):
    '''
    Read file and return year, AssessTot and PolicePrct
    '''
    year = '20' + filename[2:4]
    lines = sc.textFile('./PLUTO/' + filename, 1)
    header = lines.first()
    #lines = lines.filter(lambda x: x != header)
    #lines = lines.mapPartitions(lambda x: reader(x.replace('\0', '')))
    header = header.split(',')
    header = [h.replace('"', '') for h in header]
    value_ix = header.index('AssessTot')
    precinct_ix = header.index('PolicePrct')
    newlines = lines.map(lambda x: [int(year)] + grab_columns(x, [precinct_ix, value_ix]))
    newlines = newlines.filter(lambda x: x[1] != 'PolicePrct' and 'NULL' not in x)
    newlines = newlines.map(lambda x: ((x[0], x[1]), x[2]))
    return newlines


if __name__ == "__main__":
    newlines = process_file(files[0])

    # Get the necessary information for every file
    for file in files[1:]:
        print('Processing {0}'.format(file))
        try:
            nl = process_file(file)
        except ValueError:
            continue
        # a = nl.collect()
        newlines = newlines.union(nl)

    avg_by_key = newlines \
        .mapValues(lambda v: (float(v), float(1.))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda v: v[0] / v[1])

    avg_by_key.saveAsTextFile("value_by_year_precinct.out")

'''
    
    lines = sc.textFile('./PLUTO/MN11V2.csv', 1)


'''