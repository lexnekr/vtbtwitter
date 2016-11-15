from xlsxwriter.workbook import Workbook
import sqlite3
from datetime import datetime

def xlsx(filename, connect, tdname):
    workbook = Workbook(filename, {'strings_to_urls': False})
    worksheet = workbook.add_worksheet(tdname)
    format02 = workbook.add_format()
    format02.set_num_format('dd/mm/yyyy hh:mm AM/PM')
    head_format = workbook.add_format({'bold': True, 'italic': True})
    worksheet.write(0, 0, 'Twitt ID', head_format)
    worksheet.write(0, 1, 'Text', head_format)
    worksheet.write(0, 2, 'Date/Time', head_format)
    worksheet.write(0, 3, 'Author', head_format)
    worksheet.write(0, 4, 'Section', head_format)
    worksheet.write(0, 5, 'Retweet count', head_format)
    worksheet.write(0, 6, 'Favorite count', head_format)
    worksheet.write(0, 7, 'fullinfo', head_format)
    worksheet.write(0, 8, 'Link from twitt', head_format)
    worksheet.write(0, 9, 'Real site URL', head_format)

    c=connect.cursor()
    mysel=c.execute('''select twid, twtext, date, author_screen_name, section, retweet_count, favorite_count, fullinfo, u1.src as ex, u2.src as real_ex
                    from ''' + tdname + '''
                    join url AS u1 ON ''' + tdname + '''.expanded_url = u1.id
                    join url AS u2 ON ''' + tdname + '''.real_expanded_url = u2.id''')
    for i, row in enumerate(mysel):
        for j, value in enumerate(row):
            if j == 0:
                worksheet.write_string(i+1, j, str(value))
            elif j == 2:
                worksheet.write_datetime(i+1, 2, datetime.strptime(value, "%Y-%m-%d %H:%M:%S"), format02)
            elif j ==7:
                worksheet.write_boolean(i+1, j, value)
            elif j == 8 or j == 9:
                if len(value)>255:
                    worksheet.write(i+1, j, value)
                else:
                    worksheet.write_url(i+1, j, value)
            else:
                worksheet.write(i+1, j, value)
    workbook.close()