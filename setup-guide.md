---
name: Setup Guide
title: PlanetScale database connector by Fivetran | Setup Guide
description: Read step-by-step instructions on how to connect PlanetScale with your destination using Fivetran connectors.
---

# PlanetScale Setup Guide {% badge text="Partner-Built" /%} {% availabilityBadge connector="planetscale" /%}

Follow these instructions to replicate your PlanetScale database to your destination using Fivetran.

> NOTE: This connector is [partner-built](/docs/partner-built-program). For any questions related to PlanetScale connector and its documentation, contact PlanetScale's support team. For details on SLA, see [PlanetScale's Support Overview documentation](https://planetscale.com/docs/support/support-overview).
 
-------

## Prerequisites

To connect your PlanetScale database to Fivetran, you need a PlanetScale database.

--------

## Setup instructions

### <span class="step-item">Choose connection method</span>

<details><summary>Connect directly</summary>

Fivetran connects directly to your PlanetScale database.

</details>

<details><summary>Connect using private networking</summary>

> IMPORTANT: You must have a [Business Critical plan](https://www.fivetran.com/pricing/features) to use private networking.

We support the following providers:

- AWS PrivateLink - used for VPCs and AWS-hosted or on-premises services. See our [AWS Private Link setup guide](/docs/connectors/databases/connection-options#awsprivatelink) for details.
- GCP Private Service Connect - used for VPCs and Google-hosted or on-premises services. See our [GCP Private Service Connect setup guide](/docs/connectors/databases/connection-options#googlecloudprivateserviceconnect) for details.

</details>

### <span class="step-item">Configure PlanetScale connection string</span>

1. In [PlanetScale](https://app.planetscale.com), navigate to the database you want to connect to Fivetran and click the **Connect** button.
2. Create a new password for your main branch with [read-only permissions](https://planetscale.com/docs/concepts/password-roles#overview).
3. In the **Connect with** dropdown, select **General** and make a note of the username and password. You will need them to configure Fivetran.

### <span class="step-item">Finish Fivetran configuration</span>

1. In the [connection setup form](/docs/using-fivetran/fivetran-dashboard/connectors#addanewconnection), enter a **Destination schema prefix**. This prefix applies to each replicated schema and cannot be changed once your connection is created.
2. In the **Database host name**, enter your PlanetScale instance host name (e.g., `1.2.3.4`) or domain name (e.g., `your.server.com`).
3. Enter the name of the PlanetScale database you wish to connect to.
4. Enter the **Database username**.
5. Enter the **Database password**.
6. (Optional) Provide a **Comma-separated list of shards to sync**. If your PlanetScale database is not sharded, ignore this field. If the database is sharded, by default, the PlanetScale connection downloads rows from all shards in the database. To pick which shards are synced by the connection, you can optionally provide a comma-separated list of shards in the connection configuration.
7. Set **Use Replica?** to `true` if your PlanetScale branch has a replica. VStream will connect to the primary tablet for your database, which also serves queries to your database. To lessen the load on the primary tablet, set this to `true` to make Vstream read from a replica of your database.
   > IMPORTANT: Note that only PlanetScale production branches have replica tablets. If connecting to a development branch, set `useReplica` to `false`.
8. Set **Treat tinyint(1) as boolean** to `true` to have the connection transform tinyint(1) type columns to boolean values.
9. (Optional) Copy the [Fivetran's IP addresses (or CIDR)](/docs/using-fivetran/ips) that you _must_ safelist in your firewall. Ensure that the [Fivetran IP ranges](/docs/using-fivetran/ips) are added to the password.
10. Click **Save & Test**. Fivetran tests and validates our connection to your PlanetScale database. Upon successful completion of the setup tests, you can sync your data using Fivetran.

-----

## Related articles

[<i aria-hidden="true" class="material-icons">description</i> Connector Overview](/docs/connectors/databases/planetscale)

<b> </b>

[<i aria-hidden="true" class="material-icons">account_tree</i> Schema Information](/docs/connectors/databases/planetscale#schemainformation)

<b> </b>

[<i aria-hidden="true" class="material-icons">home</i> Documentation Home](/docs/getting-started)
