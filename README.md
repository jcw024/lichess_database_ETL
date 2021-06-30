# Database Schema Design
A reference table is preferred over creating an ENUM type because it leaves the option open for us to expand/modify the values in our set in the future. That is not possible if we were to use ENUM and is generally recommended as best practice.
