module.exports = function (RED) {
  // We need to require these here to make them available to the node's logic
  const knex = require("knex");

  function PostgresMigrationRunnerNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const MIGRATIONS_TABLE_NAME = "_nodered_migrations";

    node.on("input", async function (msg, send, done) {
      // For Node-RED 1.0+
      send =
        send ||
        function () {
          node.send.apply(node, arguments);
        };
      done =
        done ||
        function (err) {
          if (err) {
            node.error(err, msg);
          }
        };

      // --- 1. Input Validation ---
      if (!msg.migrations || !Array.isArray(msg.migrations)) {
        node.status({
          fill: "red",
          shape: "dot",
          text: "msg.migrations is not an array",
        });
        return done(new Error("Input msg.migrations must be an array."));
      }

      // --- 2. Setup Knex Connection ---
      const knexConfig = {
        client: "pg",
        connection: {
          host: config.host,
          port: config.port,
          user: config.user,
          password: config.password,
          database: config.database,
        },
        // Suppress the warning about not specifying a pool size
        pool: { min: 0, max: 1 },
      };

      let db;
      try {
        db = knex(knexConfig);
        node.status({ fill: "blue", shape: "dot", text: "Connecting..." });

        // --- 3. Ensure Migrations Table Exists ---
        const hasTable = await db.schema.hasTable(MIGRATIONS_TABLE_NAME);
        if (!hasTable) {
          node.log(`Creating migrations table: ${MIGRATIONS_TABLE_NAME}`);
          await db.schema.createTable(MIGRATIONS_TABLE_NAME, (table) => {
            table.string("name").primary();
            table.integer("batch");
            table.timestamp("migration_time");
          });
        }

        // --- 4. Get Executed Migrations and Batch Number ---
        const executed = await db(MIGRATIONS_TABLE_NAME).select("name");
        const executedNames = new Set(executed.map((m) => m.name));
        const maxBatchResult = await db(MIGRATIONS_TABLE_NAME)
          .max("batch as maxBatch")
          .first();
        const batch = (maxBatchResult.maxBatch || 0) + 1;

        // --- 5. Run Pending Migrations ---
        const appliedMigrations = [];
        let skippedCount = 0;

        for (const migration of msg.migrations) {
          if (!migration.name || !migration.up) {
            throw new Error(
              "Migration object is missing 'name' or 'up' property."
            );
          }

          if (!executedNames.has(migration.name)) {
            node.log(`Applying migration: ${migration.name}`);
            node.status({
              fill: "blue",
              shape: "dot",
              text: `Applying: ${migration.name.substring(0, 20)}...`,
            });
            if (migration.setStatus) {
              migration.setStatus({
                fill: "blue",
                shape: "dot",
                text: `Applying...`,
              });
            }

            // Execute the raw SQL from the 'up' property
            try {
              await db.raw(migration.up);
            } catch (err) {
              err.message = `${migration.name} - ${err.message}`;
              if (migration.setStatus) {
                migration.setStatus({
                  fill: "red",
                  shape: "dot",
                  text: `Error.`,
                });
              }
              throw err;
            }

            // Record the migration in our tracking table
            await db(MIGRATIONS_TABLE_NAME).insert({
              name: migration.name,
              batch: batch,
              migration_time: new Date(),
            });
            appliedMigrations.push(migration.name);
            if (migration.setStatus) {
              migration.setStatus({
                fill: "green",
                shape: "dot",
                text: `Applied.`,
              });
            }
          } else {
            skippedCount++;
            if (migration.setStatus) {
              migration.setStatus({
                fill: "green",
                shape: "dot",
                text: `Up to date.`,
              });
            }
          }
        }

        // --- 6. Send Success Output ---
        const summary = `Applied ${appliedMigrations.length}, skipped ${skippedCount}.`;
        node.status({ fill: "green", shape: "dot", text: summary });
        msg.payload = {
          summary: summary,
          applied: appliedMigrations,
          skipped: skippedCount,
        };
        send(msg);
        done();
      } catch (err) {
        // --- 7. Handle Errors ---
        node.status({ fill: "red", shape: "dot", text: "Error" });
        done(err); // Pass the error to the catch node
      } finally {
        // --- 8. Cleanup ---
        if (db) {
          await db.destroy();
          node.log("Database connection closed.");
        }
      }
    });

    node.on("close", function (done) {
      // This node doesn't hold a persistent connection, so cleanup is minimal.
      // The connection is created and destroyed per-message.
      node.status({});
      done();
    });
  }

  RED.nodes.registerType(
    "postgres-migration-runner",
    PostgresMigrationRunnerNode
  );
};
