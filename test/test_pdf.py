import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from datetime import datetime
import math
date_time = datetime.now()

def _draw_as_table(df, pagesize):
    alternating_colors = [['white'] * len(df.columns), ['lightgray'] * len(df.columns)] * len(df)
    alternating_colors = alternating_colors[:len(df)]
    fig, ax = plt.subplots(figsize=pagesize)
    ax.axis('tight')
    ax.axis('off')
    the_table = ax.table(cellText=df.values,
                        rowLabels=df.index,
                        colLabels=df.columns,
                        rowColours=['lightblue']*len(df),
                        colColours=['lightblue']*len(df.columns),
                        cellColours=alternating_colors,
                        loc='center')
    return fig



def dataframe_to_pdf(df, filename, numpages=(1, 1), pagesize=(50, 4)):
  with PdfPages(filename) as pdf:
    nh, nv = numpages
    rows_per_page = len(df) // nh
    cols_per_page = len(df.columns) // nv
    for i in range(0, nh):
        for j in range(0, nv):
            page = df.iloc[(i*rows_per_page):min((i+1)*rows_per_page, len(df)),
                           (j*cols_per_page):min((j+1)*cols_per_page, len(df.columns))]
            fig = _draw_as_table(page, pagesize)
            if nh > 1 or nv > 1:
                # Add a part/page number at bottom-center of page
                fig.text(0.5, 0.5/pagesize[0],
                         "Part-{}x{}: Page-{}".format(i+1, j+1, i*nv + j + 1),
                         ha='center', fontsize=8)
            pdf.savefig(fig, bbox_inches='tight')
            
            plt.close()

# df = pd.DataFrame(np.random.random((10,3)), columns = ("col 1", "col 2", "col 3"))
df = pd.read_csv("../go_live/files/csvs/from_api/customers.csv")

dataframe_to_pdf(df, "test_" + date_time.strftime("%m%d%y_%H%M%S") + ".pdf", numpages=(math.floor(df.shape[0]/100), 1))

# fig, ax =plt.subplots(figsize=(15,4))
# ax.axis('tight')
# ax.axis('off')
# the_table = ax.table(cellText=df.values, colLabels=df.columns, loc='center')




# with PdfPages("foo.pdf") as pdf:
#      fig = _draw_as_table(df, pagesize=(11, 8.5))
#      pdf.savefig(fig, bbox_inches='tight')   
#      plt.close()

# pp = PdfPages("foo.pdf")
# pp.savefig(fig, bbox_inches='tight')
# pp.close()