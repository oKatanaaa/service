class Address:
    addressIdCreator = 1

    def __init__(self):
        self.addressId = self.addressIdCreator
        self.addressIdCreator += 1
