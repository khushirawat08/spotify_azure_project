class Reusable:
    def dropColumns(self, df, columns):
        return df.drop(*columns)

    