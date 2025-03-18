<h1 align="center">Welcome to jira_bot 👋</h1>
<p>
  <a href="https://www.npmjs.com/package/reaper" target="_blank">
    <img alt="Version" src="https://img.shields.io/npm/v/reaper.svg">
  </a>
  <a href="docs" target="_blank">
    <img alt="Documentation" src="https://img.shields.io/badge/documentation-yes-brightgreen.svg" />
  </a>
</p>

> Am automated tool for creating and updating Jira tickets. 

Jira-Bot Workflow Portfolio Project
Overview

This repository showcases the jira-bot workflow—a project that I designed and implemented to integrate API interaction, database checks, and orchestration. Although this code is based on a production system (used internally), this repository serves as a portfolio demonstration of my skills. The repository contains the full implementation of the jira-bot, while the related Reaper (ETL process) and Gobbler (data aggregation and cleaning) flows are not included here.

What it does:

The jira-bot is scheduled to run daily every evening. It checks and updates a Jira board based on the status of data stored in an AWS RDS (Postgres) database. The workflow is orchestrated using Prefect, ensuring tasks run reliably on schedule.
Architectural Context

While the jira-bot is the primary focus of this repository, it operates in a broader ecosystem that includes:

    Reaper: An ETL process that cleans raw data and loads it into an AWS RDS (Postgres) database.
    Gobbler: A tool designed for further aggregation and cleaning of data (planned for future integration).

The jira-bot leverages production-grade practices, ensuring that it integrates smoothly into our overall system while showcasing robust design and implementation.
High-Level Workflow Diagram

Below is a diagram outlining the scope of the jira-bot workflow in the context of the larger system:

Workflow Diagram

Figure: Overview of the jira-bot workflow and its context within the larger ecosystem (Reaper and Gobbler).
Workflow Details

    Daily Scheduling:

    The jira-bot is automatically executed every evening. It checks the relevant Jira board for tickets that require updates based on the latest database state.

    Jira API Integration:

    It connects to the Jira API to retrieve current ticket statuses and update information according to predefined business rules.

    Database Interaction:

    The workflow queries the production AWS RDS (Postgres) database to determine the current data state, which then drives the logic for updating Jira tickets.

    Orchestration with Prefect:

    The entire process is managed and scheduled using Prefect, providing a reliable, production-ready execution environment.


![xrd-trias-flows drawio](https://github.com/user-attachments/assets/e5830292-cafb-41ad-a971-2792901713f6)



## Author

👤 Andrew Hunt


***
_This README was generated with ❤️ by [readme-md-generator](https://github.com/kefranabg/readme-md-generator)_# JIRA_bot
