import os
import pandas as pd

class IDPOutputer:
    def __init__(self, output_dir):
        self.output_dir = output_dir

    """Save a DataFrame to a CSV file in the 'csv' folder."""
    def save_to_csv(self, df, filename):
        csv_dir = os.path.join(self.output_dir, 'csv')
        os.makedirs(csv_dir, exist_ok=True)
        csv_path = os.path.join(csv_dir, filename)
        df.to_csv(csv_path, index=False)
        print(f"Data saved to {csv_path}")
