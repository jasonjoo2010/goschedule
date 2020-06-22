# Database Storage for GoSchedule

## Initial Tables

The first thing to do is to initial tables needed by `GoSchedule` and `Distributed lock` first.

```sql
CREATE TABLE `schedule_lock` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `key` varchar(100) NOT NULL DEFAULT '',
  `value` varchar(100) NOT NULL DEFAULT '',
  `version` bigint NOT NULL DEFAULT '0',
  `created` bigint NOT NULL DEFAULT '0',
  `expire` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `key` (`key`)
) ENGINE=InnoDB;

CREATE TABLE `schedule_info` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `key` varchar(255) NOT NULL DEFAULT '',
  `value` text NULL,
  `version` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `key` (`key`)
) ENGINE=InnoDB;
```

## Namespace

Different scheduling can be separated by different namespace(or call it prefix) like `/schedule/A`, `/schedule/B`, etc. .
