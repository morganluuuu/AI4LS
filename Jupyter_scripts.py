
import pandas as pd

# Read the original CSV file into a DataFrame
original_df = pd.read_csv('/content/LUCAS-SOIL-2018.csv')

# List of land-use classes to be considered as unmanaged
unmanaged_land_classes = ['Forestry', 'Semi-natural and natural areas not in use', 'Other abandoned areas']

# Create the 'un-/managed land' column based on the list
original_df['Un-/Managed_LU'] = 1
original_df.loc[original_df['LU1_Desc'].isin(unmanaged_land_classes), 'Un-/Managed_LU'] = 0

# Print the updated DataFrame
original_df

# Save the updated DataFrame to a new CSV file
original_df.to_csv('LUCAS-SOIL-2018(managed-l).csv', index=False)



import pandas as pd

# Load the dataset
file_path = '/content/LUCAS-SOIL-2018(managed-l).csv'
soil_data = pd.read_csv(file_path)

# Replace '< LOD' with NaN and convert columns to numeric
soil_data.replace('< LOD', pd.NA, inplace=True)
numeric_columns = ['pH_CaCl2', 'pH_H2O', 'EC', 'OC', 'CaCO3', 'P', 'N', 'K']
soil_data[numeric_columns] = soil_data[numeric_columns].apply(pd.to_numeric, errors='coerce')
# Impute missing values with column means
soil_data.fillna(soil_data.mean(), inplace=True)

# Select specific columns
selected_columns = ['pH_CaCl2', 'pH_H2O', 'EC', 'OC', 'CaCO3', 'N', 'P', 'K','TH_LAT','TH_LONG']
soil_data_selected = soil_data[selected_columns]

# Take a look at the first few rows of the selected data
print(soil_data_selected.head())



import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# Create a figure with multiple subplots
fig, axes = plt.subplots(nrows=3, ncols=3, figsize=(15, 10))

# Flatten the array of axes for easy iteration
axes = axes.flatten()

# Create box plots for each column in the selected data
for i, column in enumerate(selected_columns):
    sns.boxplot(x=soil_data[column].dropna(), ax=axes[i])
    axes[i].set_title(f'Box Plot of {column}')

# Remove any unused subplots
for i in range(len(selected_columns), len(axes)):
    fig.delaxes(axes[i])

plt.tight_layout()
plt.show()

def classify_land(row):
    # Defined thresholds based on general guidelines
    N_threshold = 0.3  # Example
    P_threshold = 40   # Example
    K_threshold = 300  # Example
    EC_threshold = 2   # Example
    pH_CaCl2_min, pH_CaCl2_max = 6.0, 7.5  # Example range
    OC_threshold = 3   # Example

    # Classification logic
    if ((row['N'] > N_threshold) or
        (row['P'] > P_threshold) or
        (row['K'] > K_threshold) or
        (row['EC'] < EC_threshold) or
        (pH_CaCl2_min <= row['pH_CaCl2'] <= pH_CaCl2_max) or
        (row['OC'] > OC_threshold)):
        return 1  # Managed
    else:
        return 0  # Unmanaged

# Apply the classification function
soil_data['land_management'] = soil_data.apply(classify_land, axis=1)

soil_data



import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
# Standardize the data
# Selecting relevant columns for clustering
columns_for_clustering = ['pH_CaCl2', 'pH_H2O', 'EC', 'OC', 'CaCO3', 'N', 'P', 'K']
soil_data_clustering = soil_data[columns_for_clustering].dropna()
scaler = StandardScaler()
scaled_data = scaler.fit_transform(soil_data_clustering)

# Apply K-Means Clustering
kmeans = KMeans(n_clusters=2, random_state=0)  # Start with 2 clusters
clusters = kmeans.fit_predict(scaled_data)

# Add the cluster information to the original DataFrame
soil_data['Cluster'] = clusters

# Analyze the clusters to determine which one might be 'managed' and 'unmanaged'
cluster_centers = kmeans.cluster_centers_
print(cluster_centers)

# Assuming cluster 0 is 'unmanaged' and cluster 1 is 'managed' (this needs to be verified)
soil_data['Land_Management'] = soil_data['Cluster'].map({0: 'Unmanaged', 1: 'Managed'})

# Assuming the output you provided is stored in 'cluster_centers'
cluster_centers = [
    [-0.88902485, -0.88223736, -0.1617712, 0.31388338, -0.32145378, 0.27200305, -0.07166017, -0.39362902],
    [0.83525465, 0.82887768, 0.15198692, -0.29489901, 0.30201154, -0.2555517, 0.06732601, 0.36982146]
]

# Assuming the first centroid (Cluster 0) represents 'unmanaged' and the second (Cluster 1) 'managed'
# (You need to confirm this based on the interpretation of the centroids)
land_management_labels = {0: 'Unmanaged', 1: 'Managed'}

# Assign labels to each data point in your dataset
soil_data['land_management'] = soil_data['Cluster'].map(land_management_labels)

# Now, you can analyze the labeled dataset further
print(soil_data['land_management'].value_counts())
print(soil_data['Un-/Managed_LU'].value_counts())

soil_data_selected



from sklearn.cluster import KMeans
import folium
import pandas as pd

# Example dataset
#data = {
#    'Latitude': [47.5, 48.5, 49.5],
#    'Longitude': [16.5, 17.5, 18.5],
#    'pH': [3.0, 4.0, 5.0],
#    'CaCO3': [2.0, 2.5, 3.0]
#}
#df = pd.DataFrame(data)

# Perform clustering (here we use k-means as an example)
kmeans = KMeans(n_clusters=2)  # 2 clusters for managed and unmanaged land
features = soil_data_selected[['pH_CaCl2', 'pH_H2O', 'EC', 'OC', 'CaCO3', 'N', 'P', 'K']]  # Replace with the features you want to use for clustering
kmeans.fit(features)
soil_data_selected['Cluster'] = kmeans.labels_

# Ensure 'Cluster' column is of type int
soil_data_selected['Cluster'] = soil_data_selected['Cluster'].astype(int)

# Export to a csv file
soil_data_selected[['Cluster','TH_LAT','TH_LONG']].to_csv('cluster_index.csv', index=False)

# Create a map object
m = folium.Map(location=[soil_data_selected['TH_LAT'].mean(), soil_data_selected['TH_LONG'].mean()], zoom_start=6)

# Color mapping for clusters
colors = ['red', 'blue']

# Add points to the map with cluster-based color
for idx, row in soil_data_selected.iterrows():
    cluster_index = int(row['Cluster'])  # Explicitly convert to int
    folium.CircleMarker(
        location=[row['TH_LAT'], row['TH_LONG']],
        radius=5,
        color=colors[cluster_index],
        fill=True,
        fill_color=colors[cluster_index],
        fill_opacity=0.6,
        popup=(f'Lat: {row["TH_LAT"]}, Lon: {row["TH_LONG"]}, '
               f'Cluster: {"Managed" if cluster_index == 0 else "Unmanaged"}')
    ).add_to(m)

# Display the map
m



from sklearn.cluster import KMeans
from joblib import dump
import pandas as pd

# Sample data for training
data = {
    'Feature1': [1.0, 1.2, 0.8, 3.5, 3.7, 3.2],
    'Feature2': [1.1, 0.9, 1.3, 3.8, 3.4, 3.6]
}
df_train = pd.DataFrame(data)

# Initialize and fit the K-Means model
kmeans_model = KMeans(n_clusters=2, random_state=42)
kmeans_model.fit(df_train)

# Save the trained model to a file
model_filename = 'kmeans_model.joblib'
dump(kmeans_model, model_filename)

print("Model training complete and saved to", model_filename)



from joblib import load

# Load the model from the file
kmeans_model_loaded = load(model_filename)

print("Model loaded successfully")

# Sample new data for prediction
new_data = {
    'Feature1': [1.1, 3.6],
    'Feature2': [1.0, 3.8]
}
df_new = pd.DataFrame(new_data)

# Use the loaded model to predict the cluster of the new data
new_predictions = kmeans_model_loaded.predict(df_new)

# Print the predictions
print("Predictions for new data:", new_predictions)
