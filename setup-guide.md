---
name: PlanetScale Setup Guide
title: PlanetScale database connector by Fivetran | Setup Guide
description: Read step-by-step instructions on how to connect PlanetScale with your destination using Fivetran connectors.
---

# PlanetScale Setup Guide {% typeBadge connector="planetscale" /%} {% availabilityBadge connector="planetscale" /%}

Follow these instructions to replicate your PlanetScale database to your destination using Fivetran.

> NOTE: This connector is [partner-built](/docs/partner-built-program). For any questions related to PlanetScale connector and its documentation, contact PlanetScale's support team. For details on SLA, see [PlanetScale's Support Overview documentation](https://planetscale.com/docs/support/support-overview).
 
-------

## Prerequisites

To connect your PlanetScale database to Fivetran, you need a PlanetScale database

--------

## Setup instructions

### <span class="step-item">Choose connection method</span>

#### Connect directly

Fivetran connects directly to your PlanetScale database.

#### Connect using private networking

> IMPORTANT: You must have a Business Critical plan to use private networking.

We support the following providers:

- AWS PrivateLink - used for VPCs and AWS-hosted or on-premises services. See our [AWS Private Link setup guide](/docs/databases/connection-options#awsprivatelink) for details.
- GCP Private Service Connect - used for VPCs and Google-hosted or on-premises services. See our [GCP Private Service Connect setup guide](/docs/databases/connection-options#googlecloudprivateserviceconnect) for details.

### <span class="step-item">Configure PlanetScale connection string</span>

1. In [PlanetScale](https://app.planetscale.com), navigate to the database you want to connect to Fivetran and click the **Connect** button.

2. Create a new password for your main branch with [read-only permissions](https://planetscale.com/docs/concepts/password-roles#overview).

3. In the **Connect with** dropdown, select **General** and leave this tab open, as you'll need to copy these credentials shortly.

### <span class="step-item">Finish Fivetran configuration</span>

Back in Fivetran, in your [connector setup form](/docs/using-fivetran/fivetran-dashboard/connectors#addanewconnector), enter the connector values as follows:

1. **Destination schema** - this prefix applies to each replicated schema and cannot be changed once your connector is created. Note: Each replicated schema is appended with `_planetscale` at the end of your chosen name.
2. **Database host name** - paste in the copied value for `host`.
3. **Database name** - paste in the copied value for `database`.
4. **Database username** - paste in the copied value for `username`.
5. **Database password** - paste in the copied value for `password`.
6. **Comma-separated list of shards to sync (optional)** - if your PlanetScale database is *not* sharded, ignore this field. If the database is sharded, by default, the PlanetScale connector will download rows from all shards in the database. To pick which shards are synced by the connector, you can optionally provide a comma-separated list of shards in the connector configuration.
7. **Use replica?**: In PlanetScale, VStream will connect to the primary tablet for your database, which also serves queries to your database. To lessen the load on the primary tablet, set this to `true` to make Vstream read from a replica of your database.
   - Please note that only PlanetScale production branches have replica tablets. If connecting to a development branch, please set `useReplica` to `false`.
8. **Treat tinyint(1) as boolean (optional)** - you can choose to have the connector transform tinyint(1) type columns in your database to either `true` or `false`.
9. **Fivetran IPs (optional)** - if your connection string was created with [IP restrictions](https://planetscale.com/docs/concepts/connection-strings#ip-restrictions), ensure that the [Fivetran IP ranges](/docs/using-fivetran/ips) are added to the password.

10. Click **Save & Test**. Fivetran tests and validates our connection to your PlanetScale database. Upon successful completion of the setup tests, you can sync your data using Fivetran.

_____

## Related articles

[<i aria-hidden="true" class="material-icons">description</i> Connector Overview](/docs/databases/planetscale)

<b> </b>

[<i aria-hidden="true" class="material-icons">account_tree</i> Schema Information](/docs/databases/planetscale#schemainformation)

<b> </b>

[<i aria-hidden="true" class="material-icons">home</i> Documentation Home](/docs/getting-started)
