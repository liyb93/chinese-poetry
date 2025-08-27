create table poems
(
    id         integer primary key autoincrement,
    category   varchar(50) not null,
    dynasty    varchar(50) not null,
    title      varchar(200)  default null,
    author     varchar(200)  default null,
    rhythmic   varchar(200)  default null,
    chapter    varchar(200)  default null,
    section    varchar(200)  default null,
    notes      varchar(2000) default null,
    paragraphs varchar(5000) default null
);