module.exports = function (RED) {
  function PostgresMigrationQueryNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const setStatus = (status) => node.status(status);

    node.on("input", function (msg, send, done) {
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

      const name = config.name;
      const up = config.SQL;

      if (!name || !up) {
        node.status({
          fill: "red",
          shape: "ring",
          text: "Missing migration name or SQL",
        });
        return done();
      }

      const migrations = Array.isArray(msg.migrations) ? msg.migrations : [];
      migrations.push({ name, up, setStatus });
      msg.migrations = migrations;

      node.status({
        fill: "green",
        shape: "dot",
        text: ``,
      });
      send(msg);
      done();
    });

    node.on("close", function (done) {
      node.status({});
      done();
    });
  }

  RED.nodes.registerType(
    "postgres-migration-query",
    PostgresMigrationQueryNode
  );
};
