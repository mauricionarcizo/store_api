from typing import List
from uuid import UUID
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
import pymongo
from store.db.mongo import db_client
from store.models.product import ProductModel
from store.schemas.product import ProductIn, ProductOut, ProductUpdate, ProductUpdateOut
from store.core.exceptions import NotFoundException, InsertionFailedException, InternalServerError
from datetime import datetime
from bson import Decimal128


class ProductUsecase:
    def __init__(self) -> None:
        self.client: AsyncIOMotorClient = db_client.get()
        self.database: AsyncIOMotorDatabase = self.client.get_database()
        self.collection = self.database.get_collection("products")

    async def create(self, body: ProductIn) -> ProductOut:
        try:
            product_model = ProductModel(**body.model_dump())
            await self.collection.insert_one(product_model.model_dump())
            return ProductOut(**product_model.model_dump())
        except Exception as e:
            print(f"Error occurred while creating product: {e}")
            raise InsertionFailedException("Failed to insert product")

    async def get(self, id: UUID) -> ProductOut:
        result = await self.collection.find_one({"id": id})

        if not result:
            raise NotFoundException(message=f"Product not found with filter: {id}")

        try:
            return ProductOut(**result)
        except Exception as e:
            print(f"Error occurred while creating ProductOut: {e}")
            raise InternalServerError()

    async def query(self) -> List[ProductOut]:
        return [ProductOut(**item) async for item in self.collection.find()]

    async def update(self, id: UUID, body: ProductUpdate) -> ProductUpdateOut:
        try:
            current_time = datetime.utcnow()
            body.updated_at = current_time 
            result = await self.collection.find_one_and_update(
                filter={"id": id},
                update={"$set": body.model_dump(exclude_none=True)},
                return_document=pymongo.ReturnDocument.AFTER,
            )
            if not result:
                raise NotFoundException(message=f"Product not found with id: {id}")
            return ProductUpdateOut(**result)
        except Exception as e:
            print(f"Error occurred while updating product: {e}")
            raise InternalServerError("Failed to update product")
    
    async def filtered_query(
        self, min_price: float = 5000, max_price: float = 8000
    ) -> List[ProductOut]:
        try:
            min_price_decimal = Decimal128(str(min_price))
            max_price_decimal = Decimal128(str(max_price))

            cursor = self.collection.find({
                "price": {"$gt": min_price_decimal, "$lt": max_price_decimal}
            })

            products = [ProductOut(**product) async for product in cursor]

            return products
        except Exception as e:
            print(f"Error occurred while querying products: {e}")
            raise InternalServerError()

    async def delete(self, id: UUID) -> bool:
        try:

            result = await self.collection.delete_one({"id": id})

            return True if result.deleted_count > 0 else False

        except Exception as e:
            print(f"Error occurred while deleting product: {e}")
            raise InternalServerError()


product_usecase = ProductUsecase()
