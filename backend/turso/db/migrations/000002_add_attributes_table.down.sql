ALTER TABLE `activities` ADD COLUMN `attributes` BLOB NULL;
UPDATE `activities` SET `attributes` = `attributes`.`data` FROM `attributes` WHERE `activities`.`id` = `attributes`.`id` AND `activities`.`instance_id` = `attributes`.`instance_id` AND `activities`.`execution_id` = `attributes`.`execution_id`;

ALTER TABLE `history` ADD COLUMN `attributes` BLOB NULL;
UPDATE `history` SET `attributes` = `attributes`.`data` FROM `attributes` WHERE `history`.`id` = `attributes`.`id` AND `history`.`instance_id` = `attributes`.`instance_id` AND `history`.`execution_id` = `attributes`.`execution_id`;

ALTER TABLE `pending_events` ADD COLUMN `attributes` BLOB NULL;
UPDATE `pending_events` SET `attributes` = `attributes`.`data` FROM `attributes` WHERE `pending_events`.`id` = `attributes`.`id` AND `pending_events`.`instance_id` = `attributes`.`instance_id` AND `pending_events`.`execution_id` = `attributes`.`execution_id`;

-- Drop attributes table
DROP TABLE `attributes`;