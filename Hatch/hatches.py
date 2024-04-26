import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
import os
from concurrent.futures import ThreadPoolExecutor

def find_direction(
        df,             # DataFrame filtered to a single layer
        plot=False      # Whether to plot a visualization
):
    # Filter for values where 'hatch' is at its first and last index
    first_hatch = df[df['hatch'] == df['hatch'].iloc[0]]
    last_hatch = df[df['hatch'] == df['hatch'].iloc[-1]]

    # Calculate the centroid of each group
    first_centroid = np.array([first_hatch['x'].mean(), first_hatch['y'].mean()])
    last_centroid = np.array([last_hatch['x'].mean(), last_hatch['y'].mean()])

    # Compute the angle between the centroids
    vector_to_last_centroid = last_centroid - first_centroid
    angle_radians = np.arctan2(vector_to_last_centroid[1], vector_to_last_centroid[0])
    angle_degrees = np.degrees(angle_radians)

    # Round to the nearest 90 degrees
    angle_degrees = int(np.round(angle_degrees / 90) * 90)

    if angle_degrees == 180 or angle_degrees == -180:
        direction = 'West'
    elif angle_degrees == 90 or angle_degrees == -270:
        direction = 'North'
    elif (angle_degrees == 0 or angle_degrees == -0 or
          angle_degrees == 360 or angle_degrees == -360):
        direction = 'East'
    elif angle_degrees == -90 or angle_degrees == 270:
        direction = 'South'
    else:
        direction = 'Unknown'
        warnings.warn(f'Unknown direction: {angle_degrees} degrees')

    if plot:
        # Scatter x and y values for df
        plt.scatter(first_centroid[0], first_centroid[1],
                    color='red', label='First Hatch')
        plt.scatter(last_centroid[0], last_centroid[1],
                    color='blue', label='Last Hatch')
        plt.legend()

    return direction, angle_degrees


def plot_layer_hatches(
        df,            # DataFrame filtered to a single layer
        signal         # Column name to plot i.e. 'pyro2'
):
    # Convert hatch values to millimeters
    df = df.copy()
    df['hatch_mm'] = (df['hatch'] - 1) * 75e-3

    # Group by 'hatch_mm' and calculate the mean of 'pyro2'
    pyro = df.groupby('hatch_mm')[signal].mean()

    # Round pyro and make int
    pyro = pyro.round().astype(int)

    # print(f'data type of pyro: {pyro.dtype}')

    # Group 'length' by 'hatch_mm'
    length = df.groupby('hatch_mm')['hatch_mm'].mean()



    return pyro, length

def layer_df(
        df,             # DataFrame for a single layer
        signal          # Column name to plot i.e. 'pyro2'
             ):
    # Plot the average 'pyro2' value for each hatch
    pyro, length = plot_layer_hatches(df, signal)

    # Find the direction of the layer
    direction, angle = find_direction(df, plot=False)

    results = pd.DataFrame({
        'direction': direction,
        'signal': pyro,
        'length': length
    })

    direction_categories = ['East', 'West', 'North', 'South', 'Unknown']
    results['direction'] = results['direction'].astype(pd.CategoricalDtype(categories=direction_categories))

    return results



def layer_check(df, layer_num):

    # Filter for a single layer
    df = df[df['layer'] == layer_num]

    # create a DataFrame for the layer
    results = layer_df(df, 'pyro2')

    # from the DataFrame, plot pyro2 vs length
    plt.figure(figsize=(10, 6))
    for direction in results['direction'].cat.categories:
        direction_df = results[results['direction'] == direction]
        plt.plot(direction_df['length'], direction_df['signal'], label=direction)
    plt.xlabel('Hatch Length (mm)')
    plt.ylabel('Average Pyro2 Value')
    plt.title('Average Pyro2 Value vs. Hatch Length')
    plt.legend()
    plt.grid()
    plt.show()


def plot_part(df):

    # Define colors for each direction
    colors = {
        'North': 'tab:red',
        'East': 'tab:green',
        'South': 'tab:blue',
        'West': 'tab:olive',
        'Unknown': 'tab:gray'  # Color for any unexpected or undefined directions
    }

    # Initialize a figure for plotting individual layer data
    plt.figure(figsize=(10, 6))

    # Print the unique layers
    print(f"Unique layers: {df['layer'].unique()}")

    # Data container for averages
    direction_averages = {direction: [] for direction in colors.keys()}

    # Plot for each unique layer...
    for layer_num in df['layer'].unique():
        # Filter for a single layer without modifying the original DataFrame
        df_layer = df[df['layer'] == layer_num]

        # Check if the DataFrame is not empty
        if not df_layer.empty:
            # Create a DataFrame for the layer
            results = layer_df(df_layer, 'pyro2')

            # Check if results DataFrame is not empty
            if not results.empty:
                # Overlay pyro2 vs length using color coding for direction
                for direction in results['direction'].cat.categories:
                    if direction in colors:  # Ensure the direction has a defined color
                        direction_df = results[results['direction'] == direction]
                        if not direction_df.empty:
                            plt.plot(direction_df['length'], direction_df['signal'],
                                     color=colors[direction], alpha=0.3, label=direction if layer_num == 1 else "")
                            # Aggregate data for average calculation
                            direction_averages[direction].append(direction_df['signal'].mean())

        else:
            print(f"No data for layer {layer_num}. Skipping...")

    # Remove duplicate labels in the legend
    handles, labels = plt.gca().get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    plt.legend(by_label.values(), by_label.keys())

    plt.xlabel('Hatch Length (mm)')
    plt.ylabel('Average Pyro2 Value')
    plt.title('Average Pyro2 Value vs. Hatch Length Across All Layers')
    plt.grid()
    plt.show()

def part_avg(df, plot=False):
    # Define colors for each direction
    colors = {
        'North': 'tab:red',
        'East': 'tab:green',
        'South': 'tab:blue',
        'West': 'tab:olive',
        'Unknown': 'tab:gray'  # Color for any unexpected or undefined directions
    }

    part_df = pd.DataFrame()

    # Get result dataframes for each layer
    for layer_num in df['layer'].unique():
        df_layer = df[df['layer'] == layer_num]
        results = layer_df(df_layer, 'pyro2')
        if not results.empty: # then add the results to the part_df without using append
            part_df = pd.concat([part_df, results])

    # print(part_df.info())

    # For each category, calculate the average signal for each value of 'length'
    avg_df = part_df.groupby(['direction', 'length'])['signal'].mean().reset_index()

    if plot:
        # Initialize a figure for plotting the average data
        plt.figure(figsize=(10, 6))

        # Plot the average signal for each direction
        for direction in avg_df['direction'].cat.categories:
            direction_df = avg_df[avg_df['direction'] == direction]
            plt.plot(direction_df['length'], direction_df['signal'], color=colors[direction], label=direction)

        plt.xlabel('Hatch Length (mm)')
        plt.ylabel('Average Pyro2 Value')
        plt.title('Average Pyro2 Value vs. Hatch Length Across All Layers')
        plt.legend()
        plt.grid()
        plt.show()

    return avg_df


def process_file(file, read_dir, save_dir):
    start_time = time.time()

    # Load the data
    df = pd.read_parquet(os.path.join(read_dir, file))
    avg_df = part_avg(df, plot=False)  # Assuming part_avg is a function you've defined elsewhere

    # Save the average data to a new .parquet file
    save_path = os.path.join(save_dir, f'{file.split(".")[0]}_hatch.parquet')
    avg_df.to_parquet(save_path)

    print(f'{file} saved in {time.time() - start_time:.2f} seconds.')


def save_part_avg(read_dir, save_dir):
    # Create the save directory if it does not exist
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    files = [file for file in os.listdir(read_dir) if file.endswith('.parquet') and file.split('.')[0].isdigit()]

    # Use ThreadPoolExecutor to handle file processing in parallel
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(process_file, file, read_dir, save_dir) for file in files]

        # Optionally, if you want to ensure all tasks are completed before moving on:
        for future in futures:
            future.result()  # This line will block until the particular task is finished

    print("All data saved. Whoop!")


def main():
    read_dir = '/mnt/parscratch/users/eia19od/Cleaned'
    save_dir = '/mnt/parscratch/users/eia19od/hatches'
    save_part_avg(read_dir, save_dir)

    #







if __name__ == '__main__':
    main()


# Unique angles in sample 2: [-180  -90    0   90  180]
# Unique angles in sample 3: [-180  -90    0   90]
# Unique angles in sample 4: [-180  -90    0   90]
# Unique angles in sample 5: [-180  -90    0   90]

# 180 degree is West
# 90 degree is North
# 0 degree is East
# -90 degree is South





