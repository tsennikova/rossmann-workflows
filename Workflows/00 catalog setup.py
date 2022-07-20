# Databricks notebook source
catalog = "field_demos"
spark.sql(f"SET c.catalog = {catalog}")
