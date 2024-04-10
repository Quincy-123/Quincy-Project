from pymongo import MongoClient
from gridfs import GridFS
import gridfs
import os
import pandas as pd
import pickle
import datetime
import plotly.express as px
import streamlit as st
import io
import warnings
from io import BytesIO

# Connect to MongoDB Atlas
mongo_connection_string = st.secrets["mongo"]["connection_string"]
client = MongoClient(mongo_connection_string)
# Select the database and create a GridFS object
db = client['QuincyDB']
db_po = client['PO_Quincy']
db_invoice = client['Invoice_Quincy']
fs = gridfs.GridFS(db)


#--------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def process_data(df_po, df_invoice, start_date_str, end_date_str):
    # Convert start_date and end_date to datetime objects
    start_date = pd.to_datetime(start_date_str)
    end_date = pd.to_datetime(end_date_str)

    df_po.rename(columns={'Quantity' : 'Total Quantity'}, inplace=True)
    
    # Merge the dataframes on SKU column
    merged_df = pd.merge(df_po, df_invoice, on='SKU', how='outer')
    
    # If 'Exit Factory Date' column exists, filter data by start date and end date
    if 'Exit Factory Date' in merged_df.columns:
        merged_df['Exit Factory Date'] = pd.to_datetime(merged_df['Exit Factory Date'])
        filtered_df = merged_df[(merged_df['Exit Factory Date'] >= start_date) & (merged_df['Exit Factory Date'] <= end_date)]
    else:
        filtered_df = merged_df  # If 'Exit Factory Date' column doesn't exist, use all data
   
    if 'Product_x' in filtered_df.columns:
        # Group by SKU and calculate sum of Quantity and Total
        grouped_df = filtered_df.groupby(['SKU','Variant']).agg({'Total Quantity': 'sum', 'Quantity': 'sum','Product_x': 'first'})
    elif 'Product' in filtered_df.columns:
        # Group by SKU and calculate sum of Quantity and Total
        grouped_df = filtered_df.groupby(['SKU','Variant']).agg({'Total Quantity': 'sum', 'Quantity': 'sum','Product': 'first'})
    else:
        grouped_df = filtered_df.groupby(['SKU','Variant']).agg({'Total Quantity': 'sum', 'Quantity': 'sum'})
    
    # Rename columns for clarity
    grouped_df.rename(columns={'Total Quantity': 'Ordered', 'Quantity': 'Sold'}, inplace=True)

    # Calculate Sale %
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="invalid value encountered in .*")
        warnings.filterwarnings("ignore", message="invalid value encountered in scalar divide")
        grouped_df['Sale %'] = (grouped_df['Sold'] / grouped_df['Ordered']) * 100

    # Calculate totals
    total_ordered = grouped_df['Ordered'].sum()
    total_sold = grouped_df['Sold'].sum()
    total_sale_percentage = (total_sold / total_ordered) * 100

    # Add total row to the dataframe
    total_row = pd.DataFrame({'Ordered': total_ordered, 'Sold': total_sold, 'Sale %': total_sale_percentage}, index=['Total'])
    grouped_df = pd.concat([grouped_df, total_row])

    return grouped_df


def generate_pivot_tables(start_date_str, end_date_str):

    invoice_names = db_invoice.list_collection_names()
    po_names = db_po.list_collection_names()

    # Create an Excel writer object
    excel_writer = pd.ExcelWriter('PO_Report.xlsx')

    # Iterate through each purchase order
    for po_name in po_names:
        # Create an empty DataFrame to store the data
        df_combined = pd.DataFrame()
        
        # Iterate through each collection
        for collection_name in invoice_names:
            # Retrieve data from the collection where Purchase Order Id is equal to po_name
            collection_data = list(db_invoice[collection_name].find({'Purchase Order Id': int(po_name)}))
            
            # Convert retrieved data to DataFrame
            df_collection = pd.DataFrame(collection_data)
       
            # Append it to the combined DataFrame
            df_combined = pd.concat([df_combined, df_collection], ignore_index=True)
            
        if not df_combined.empty:
            # Group by 'SKU' and sum the 'Quantity' within each group
            df_grouped = df_combined.groupby(['SKU'], as_index=False)['Quantity'].sum()
            
            # Process the data using the process_data function
            processed_data = process_data(pd.DataFrame(list(db_po[po_name].find({}))), df_grouped, start_date_str, end_date_str)

            if 'Product' in processed_data.columns:
                processed_data = processed_data[['Product','Ordered','Sold','Sale %']]
                
            # Output the pivot table to Excel
            processed_data.to_excel(excel_writer, sheet_name=po_name)

    # Save the Excel file
    excel_writer._save()



#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def read_pickle_from_gridfs(filename):
    """
    Read a Pickle file from MongoDB GridFS and return the data
    """
    pickle_data = fs.find_one({"filename": filename}).read()
    data = pickle.loads(pickle_data)
    return data

def write_pickle_to_gridfs(data, filename):
    """
    Write a Pickle file to MongoDB GridFS
    """
    pickle_data = pickle.dumps(data)

    # Check if the file already exists
    existing_file = fs.find_one({"filename": filename})

    if existing_file:
        # If the file exists, delete it before creating a new one
        fs.delete(existing_file._id)

    # Create a new file
    fs.put(pickle_data, filename=filename, encoding="utf-8")


def read_excel(file):
    """
    Read the Excel file from MongoDB GridFS and return a DataFrame
    """
    # Read the raw content of the file from GridFS
    file_content = fs.find_one({"filename": file}).read()
    
    # Convert the raw content into a file-like object using io.BytesIO
    excel_file = io.BytesIO(file_content)
    
    # Use pd.read_excel to read the Excel file from the file-like object
    df = pd.read_excel(excel_file)
    
    return df

def write_excel_to_gridfs(df, filename):
    """
    Write an Excel file to MongoDB GridFS
    """
    excel_data = BytesIO()
    with pd.ExcelWriter(excel_data, engine="openpyxl", mode="w", index=False) as writer:
        df.to_excel(writer)
    
    # Check if the file already exists
    existing_file = fs.find_one({"filename": filename})

    if existing_file:
        # If the file exists, delete it before creating a new one
        fs.delete(existing_file._id)

    # Create a new file
    fs.put(excel_data.getvalue(), filename=filename, encoding="utf-8")

def read_pickle_from_gridfs(filename):
    """
    Read a Pickle file from MongoDB GridFS and return the data
    """
    pickle_data = fs.find_one({"filename": filename})
    if pickle_data is not None:
        data = pickle.loads(pickle_data.read())
        return data
    else:
        return None

def update_excel():
    """
    Updates the Excel sheet based on:
    1. Total ordered -- PO (all)
    2. Total given -- Invoice
    3. Total received -- PO (only received)

    Calculates the total ordered (PO all) Vs. total offered (PO received) % and 
    sales (total given) Vs. total offered (PO received)
    """
    
    map_sku_filename = 'mapping_SKU.pickle'
    rev_map_sku_filename = 'reverse_mapping_SKU.pickle'
    tot_given_filename = 'total_given.pickle'
    tot_ordered_filename = 'total_ordered.pickle'
    total_received_filename = 'received.pickle'

    map_sku = read_pickle_from_gridfs(map_sku_filename)
    rev_map_sku = read_pickle_from_gridfs(rev_map_sku_filename)
    tot_given = read_pickle_from_gridfs(tot_given_filename)
    tot_ordered = read_pickle_from_gridfs(tot_ordered_filename)
    total_received = read_pickle_from_gridfs(total_received_filename)

    excel_export = pd.DataFrame(columns=['SKU', 'Product Name', 'Size', 'Color', 'Total Ordered', 'Total Offered', 'Total Sales', 'Total Ordered vs Total Offered (Percentage)', 'Total Sales vs Total Offered (Percentage)'])

    rev_map_sku = dict(sorted(rev_map_sku.items(), key=lambda x: x[1]))

    for k, v in rev_map_sku.items():
        prod, size, col = k.split('_')
        sku = str(v).strip()
        total_order = tot_ordered[sku]
        total_sales = tot_given[sku]
        total_offered = total_received[sku]
        percentage_to_vs_tof = round((total_offered / total_order) * 100, 2)
        try:
            percentage_sales_vs_tof = round((total_sales / total_offered) * 100, 2)
        except ZeroDivisionError:
            percentage_sales_vs_tof = 0

        excel_export.loc[len(excel_export)] = [sku, prod, size, col, total_order, total_offered, total_sales, percentage_to_vs_tof, percentage_sales_vs_tof]

    excel_export.columns = ['SKU', 'Product Name', 'Size', 'Color', 'Total Ordered', 'Total Offered', 'Total Sales', 'Total Ordered vs Total Offered (Percentage)', 'Total Sales vs Total Offered (Percentage)']
    excel_export_filename = 'Sales_Info_updated_1.xlsx'
    excel_export.to_excel(excel_export_filename, index=False)

    # Write updated data back to GridFS
    write_pickle_to_gridfs(map_sku, map_sku_filename)
    write_pickle_to_gridfs(rev_map_sku, rev_map_sku_filename)
    write_pickle_to_gridfs(tot_given, tot_given_filename)
    write_pickle_to_gridfs(tot_ordered, tot_ordered_filename)
    write_pickle_to_gridfs(total_received, total_received_filename)
    with open(excel_export_filename, 'rb') as file:
        fs.put(file, filename=excel_export_filename)

# Upload two Excel files
st.sidebar.header("Upload Files")

# Pending PO (New PO) upload
pending_order = st.sidebar.file_uploader("Upload Pending PO", type=["xlsx"])
pending_submit = st.sidebar.button("Pending PO Submit")

# Load Pending PO, Processed PO, and All PO pickle files
pending_list = read_pickle_from_gridfs('pending_list_map.pickle')
processed_po = read_pickle_from_gridfs('processed_po.pickle')
po = read_pickle_from_gridfs('po.pickle')

# Load mapping SKU and reverse mapping SKU
map_sku = read_pickle_from_gridfs('mapping_SKU.pickle')
rev_map_sku = read_pickle_from_gridfs('reverse_mapping_SKU.pickle')

if pending_order and pending_submit:
    file_name_pending = str(pending_order.name).split('.')[0]
    if file_name_pending not in po and file_name_pending not in processed_po:
        po.append(file_name_pending)
        # Process the uploaded files 

        pending_list.append(file_name_pending)

        total_ordered_map = read_pickle_from_gridfs('total_ordered.pickle')
        filename = pending_order.name
        curr_data = pd.read_excel(pending_order)
        
        curr_data.rename(columns={'Quantity' : 'Total Quantity', 'Product Name' : 'Product'}, inplace=True)
        curr_sku = curr_data['SKU'].to_numpy()
        curr_quan = curr_data['Total Quantity'].to_numpy()
        curr_prod = curr_data['Product'].to_numpy()
        curr_size = curr_data['Size'].to_numpy()
        curr_col = curr_data['Color'].to_numpy()

        for j in range(len(curr_sku)):
            prod = curr_sku[j]
            po_quan = curr_quan[j]
            
            if prod in total_ordered_map:
                total_ordered_map[prod] += po_quan
            
            else:
                # st.write(total_ordered_map[prod])
                product = str(curr_prod[j]) + '_' + str(curr_size[j]) + '_' + str(curr_col[j])
                map_sku[str(curr_sku[j])] = str(product)
                rev_map_sku[str(product)] = str(curr_sku[j])
                st.toast(f"New Product : {str(curr_prod[j])}_{str(curr_size[j])}_{str(curr_col[j])} -> {curr_sku[j]}")
                write_pickle_to_gridfs(map_sku, 'mapping_SKU.pickle')
                write_pickle_to_gridfs(rev_map_sku, 'reverse_mapping_SKU.pickle')
                total_ordered_map[prod] = po_quan
            
        write_pickle_to_gridfs(pending_list, 'pending_list_map.pickle')
        write_pickle_to_gridfs(total_ordered_map, 'total_ordered.pickle')
        po = sorted(po)
        write_pickle_to_gridfs(po, 'po.pickle')
        #Upload the added file to the DB
        po_data = pd.read_excel(pending_order)
        po_name = os.path.splitext(pending_order)[0]
        PO[po_name].insert_many(po_data.to_dict('records'))
        #Update the data in Excel File
        update_excel()
        st.success(f'{pending_order.name} PO file has been processed successfully', icon="✅")
    
    else:
        st.error(f'{pending_order.name} has already been processed', icon="🚨")
    
elif pending_submit:
    st.error('Please upload the Pending PO file', icon="🚨")

# received order

received_order = st.sidebar.file_uploader("Upload PO to be Offered", type=["xlsx"])
received_submit = st.sidebar.button("Offer")

if received_order and received_submit:
    file_name_received = str(received_order.name).split('.')[0]
    if file_name_received in pending_list:
        # Process the uploaded files
        pending_list.remove(file_name_received)
        processed_po.append(file_name_received)
        
        processed_po = sorted(processed_po)
        
        pending_list = sorted(pending_list)    

        total_received_map = read_pickle_from_gridfs('received.pickle')
        filename = received_order.name
        curr_data = pd.read_excel(received_order)

        curr_data.rename(columns={'Quantity' : 'Total Quantity'}, inplace=True)
        curr_sku = curr_data['SKU'].to_numpy()
        curr_quan = curr_data['Total Quantity'].to_numpy()

        for j in range(len(curr_sku)):
            prod = curr_sku[j]
            po_quan = curr_quan[j]

            if prod in total_received_map:
                # total_received_map[prod]
                total_received_map[prod] += po_quan
            else:
                total_received_map[prod] = po_quan
        
        write_pickle_to_gridfs(processed_po, 'processed_po.pickle')
        write_pickle_to_gridfs(pending_list, 'pending_list_map.pickle')
        write_pickle_to_gridfs(total_received_map, 'received.pickle')
        #Upload the added file to the DB
        po_data = pd.read_excel(received_order)
        po_name = os.path.splitext(received_order)[0]
        PO[po_name].insert_many(po_data.to_dict('records'))
        #Update the Excel File
        update_excel()
        st.success(f'{received_order.name} PO file has been processed successfully', icon="✅")
    
    else:
        st.error(f'{received_order.name} is not in the Pending PO\'s', icon="🚨")

elif received_submit:
    st.error('Please upload the Received PO file', icon="🚨")

invoice_excel = st.sidebar.file_uploader("Upload Invoice File", type=["xlsx"])
invoice_submit = st.sidebar.button("Invoice Submit")

invoice_list = read_pickle_from_gridfs('invoice_list_map.pickle')

# Invoice Processing
if invoice_excel and invoice_submit:
    file_name_invoice = invoice_excel.name.split('.')[0].split(' ')[0]
    if file_name_invoice not in invoice_list:
        invoice_list.append(file_name_invoice)
        df1 = pd.read_excel(invoice_excel)

        invoice_pickle = read_pickle_from_gridfs('total_given.pickle')

        sku = df1['SKU'].to_numpy()
        quan = df1['Quantity'].to_numpy()

        for i in range(len(sku)):
            if sku[i] in invoice_pickle:
                # invoice_pickle[sku[i]]
                invoice_pickle[sku[i]] += quan[i]
            else:
                invoice_pickle[sku[i]] = quan[i]

        write_pickle_to_gridfs(invoice_pickle, 'total_given.pickle')
        invoice_list = sorted(invoice_list)
        write_pickle_to_gridfs(sorted(invoice_list), 'invoice_list_map.pickle')
        #Upload the added file to the DB
        invoice_data = pd.read_excel(invoice_excel)
        invoice_name = os.path.splitext(pinvoice_excel)[0]
        Invoice[invoice_name].insert_many(invoice_data.to_dict('records'))
        #Update the excel file
        update_excel()
        st.success(f'{invoice_excel.name} file has been processed successfully', icon="✅")
    else:
        st.error(f'{invoice_excel.name} invoice file is already processed', icon="🚨")

elif invoice_submit:
    st.error('Please upload the invoice file', icon="🚨")

# generate excel
generate_excel = st.sidebar.button("Generate Excel")
if generate_excel:  
    update_excel()

    # Specify the Excel file name in GridFS
    excel_filename = "Sales_Info_updated_1.xlsx"

    # Read the Excel file from GridFS
    file_content = fs.get_version(excel_filename, -1).read()

    # Create a BytesIO object to simulate reading from a file
    file_io = io.BytesIO(file_content)

    # Provide a filename for the download button
    download_filename = f"{datetime.datetime.now()}_Sales_Info.xlsx"

    # Display the download button
    btn = st.sidebar.download_button( 
        label="Download Report",
        data=file_io,
        file_name=download_filename,
        mime="xlsx"
    )

# visualization
# Function to filter the data based on input_value
def filter_data(data, input_value):
    return data.query("OfferedPercent <= @input_value")

def filter_non_zeros(data):
    return data.query("OfferedPercent > 0")

def filter_zeros(data):
    return data.query("OfferedPercent == 0")

def zero_sales(data):
    return data.query("OfferedPercent > 0 and SalesPercent == 0")

# Upload Excel file
data = read_excel('Sales_Info_updated_1.xlsx')
data = data.rename(columns={'Total Ordered vs Total Offered (Percentage)': 'OfferedPercent', 'Total Sales vs Total Offered (Percentage)' : 'SalesPercent', 'Product Name': 'name'})

st.title('Quincy Data Visualization')
# Input value slider
input_value = st.slider(f"**Select the threshold percentage for offered:**", min_value=0.0, max_value=100.0, value=75.0, step=1.00)

# Filter the data based on input_value
total_products = len(data)

filtered_data = filter_data(data, input_value)
filtered_products = len(filtered_data)

greater_than_threshold = total_products - filtered_products

filtered_data_non_zeros = filter_non_zeros(filtered_data)
zero_products = filtered_products - len(filtered_data_non_zeros)

filtered_data_zeros = filter_zeros(filtered_data)

filtered_data_zero_sales = zero_sales(filtered_data)

# Show the filtered data in a table
with st.expander(f'**See Filtered Data**'):
    st.write(f"**Filtered Data:**")
    st.write(filtered_data)
    st.write("Total Number of SKUs : ", str(len(filtered_data)))

with st.expander(f'**See Zero Percent Offered SKUs**'):
    st.write(f'**Zero Percent Offered Products**')
    st.write(filtered_data_zeros)
    st.write("Total Number of SKUs : ", str(len(filtered_data_zeros)))

with st.expander(f'**See Zero Sales SKUs**'):
    st.write(f'**Offered SKUs with Zero Percent Sales**')
    st.write(filtered_data_zero_sales)
    st.write("Total Number of SKUs : ", str(len(filtered_data_zero_sales)))

col1, col2 = st.columns([0.4, 0.6], gap='medium')
with col1:
    st.write(f"**Total Products : {total_products}**")
    st.markdown(f"**:red[Zero % Offered] : :red[{zero_products}]**")
    
with col2:
    st.markdown(f"**Products with Offered % :red[Less Than {input_value} %] : :red[{filtered_products}]**")
    st.markdown(f"**Products with Offered % :green[Greater Than {input_value} %] : :green[{greater_than_threshold}]**")

col3, col4, col5 = st.columns(3, gap='small')
with col3:
    with st.expander(f"**Show Pending PO's**"):
        st.dataframe(pending_list, hide_index=True, width=500, column_config={'value' : 'Pending PO'})

with col4:
    processed_po = read_pickle_from_gridfs('processed_po.pickle')
    with st.expander(f"**Show Processed PO's**"):
        st.dataframe(processed_po, hide_index=True, width=500, column_config={'value' : 'Processed PO\'s'})

with col5:
    invoice_list = read_pickle_from_gridfs('invoice_list_map.pickle')
    with st.expander(f'**Show Processed Invoice**'):
        st.dataframe(invoice_list, hide_index=True, width=500, column_config={'value' : 'Processed Invoice'})

# Data visualization using Plotly Express with custom color mapping and increased bar width
fig = px.bar(filtered_data_non_zeros, x='SKU', y='OfferedPercent', hover_data=['name', 'Size', 'Color'],
                color='OfferedPercent', color_continuous_scale=[(0.0, 'darkred'), (0.4, 'red'), (0.7, 'lightgreen'), (1.0, 'green')], height=500, width=1000,
                color_continuous_midpoint=50)
fig.update_xaxes(
        tickangle = 45,
        title_text = "SKU",
        title_font = {"size": 20},
        title_standoff = 25)

fig.update_yaxes(
        title_text = "Offered %",
        title_font = {"size": 20},
        title_standoff = 25)

fig2 = px.bar(filtered_data_non_zeros, x='SKU', y='SalesPercent', hover_data=['name', 'Size', 'Color'],
                color='SalesPercent', color_continuous_scale=[(0.0, 'darkred'), (0.4, 'red'), (0.7, 'lightgreen'), (1.0, 'green')], height=500, width=1000,
                color_continuous_midpoint=50)

fig2.update_xaxes(
        tickangle = 45,
        title_text = "SKU",
        title_font = {"size": 20},
        title_standoff = 25)

fig2.update_yaxes(
        title_text = "Sales %",
        title_font = {"size": 20},
        title_standoff = 25)

# Increase the width of the bars
fig.update_layout(barmode='group', bargap=0.1, title=f'Total Ordered vs Total Offered (Percentage) Less than {input_value}%', title_font = {"size": 20})
fig2.update_layout(barmode='group', bargap=0.1, title=f'Total Sales vs Total Offered (Percentage)', title_font = {"size": 20})

st.plotly_chart(fig)
st.plotly_chart(fig2)


# Create date input widgets in the sidebar
start_date = st.sidebar.date_input("Start Date", datetime.date(2022, 1, 1))
end_date = st.sidebar.date_input("End Date", datetime.date(2024, 4, 10))

# Button to generate PO wise data
generate_excel_po = st.sidebar.button("Generate PO Wise Data")

if generate_excel_po:
    # Format the dates
    formatted_start_date = start_date.strftime("%Y-%m-%d")
    formatted_end_date = end_date.strftime("%Y-%m-%d")
    
    # Call the function with formatted dates
    generate_pivot_tables(formatted_start_date, formatted_end_date)
    
    # Provide a filename for the download button
    excel_filename = "PO_Report.xlsx"
    # Read the Excel file
    excel_data = pd.read_excel(excel_filename)

    # Write Excel data to GridFS
    # write_excel_to_gridfs(excel_data, excel_filename) // To DO for now
    # Read the Excel file
    file_content = open(excel_filename, "rb").read()

    # Create a BytesIO object to simulate reading from a file
    file_io = io.BytesIO(file_content)

    # Provide a filename for the download button
    download_filename = f"Sales_Report_PO_Wise_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"

    # Display the download button
    btn = st.sidebar.download_button( 
        label="Download PO-Report",
        data=file_io,
        file_name=download_filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
