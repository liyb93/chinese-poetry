package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"github.com/Sloaix/opencc"
	"github.com/buger/jsonparser"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

var workDir, _ = os.Getwd()

var openccRepoDir = filepath.Join(workDir, "opencc")     // 简繁转换repo
var openccDataDir = filepath.Join(openccRepoDir, "data") //
var repoDir = filepath.Join(workDir, "repo")             // 诗歌的repo
var outputDir = filepath.Join(workDir, "output")

var sqlDir = filepath.Join(outputDir, "sql")
var sqlFileHansPath = filepath.Join(sqlDir, "poems_hans.sql")
var sqlFileHantPath = filepath.Join(sqlDir, "poems_hant.sql")
var databaseDir = filepath.Join(outputDir, "database")
var databaseFilePath = filepath.Join(databaseDir, "poems.db")
var counter = 1

type JsonFile struct {
	filePath string
	category string
	dynasty  string
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func cleanup() {
	var directories = [...]string{outputDir, sqlDir, databaseDir}

	for _, dir := range directories {
		_ = os.RemoveAll(dir)
		fmt.Printf("remove dir %s \n", dir)

		_ = os.MkdirAll(dir, os.ModePerm)
		fmt.Printf("make dir %s \n", dir)
	}
}

func shell(dir string, cmd string) {
	fmt.Printf("Excute Command => %s \n", cmd)
	command := exec.Command("/bin/bash", "-c", cmd)
	command.Dir = dir
	output, err := command.Output()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s \n", output)
}

// 克隆代码仓库
func cloneRepo() {
	// 诗词仓库
	if fileExists(repoDir) {
		shell(repoDir, "git fetch origin master")
		shell(repoDir, "git reset --hard origin/master")
	} else {
		shell(workDir, "git clone https://github.com/Sloaix/chinese-poetry.git repo")
	}

	// 简繁转换仓库
	if fileExists(openccRepoDir) {
		shell(openccRepoDir, "git fetch origin master")
		shell(openccRepoDir, "git reset --hard origin/master")
	} else {
		shell(workDir, "git clone https://github.com/Sloaix/opencc")
	}
}

// 创建数据和和表
func createDatabaseAndTable() {
	println("createDatabaseAndTable")
	db, err := sql.Open("sqlite3", databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	createTableSql, err := ioutil.ReadFile("./create_table.sql")
	if err != nil {
		log.Fatal(err)
	}

	sqlStmt := string(createTableSql)
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return
	}
}

func createJsonFiles(dir string, glob string, category string, dynasty string) []JsonFile {
	fileGlob := filepath.Join(repoDir, dir, glob)
	jsonFilePaths, _ := filepath.Glob(fileGlob)
	jsonFiles := make([]JsonFile, 0)
	for _, filePath := range jsonFilePaths {
		jsonFiles = append(jsonFiles, JsonFile{
			filePath: filePath,
			category: category,
			dynasty:  dynasty,
		})
	}
	return jsonFiles
}

// 解析所有的json文件
func parseJson() {
	println("parseJson")
	jsonFiles := importToJsonFiles()
	for _, jsonFile := range jsonFiles {

		isYuanqu := strings.HasSuffix(jsonFile.filePath, "yuanqu.json")

		// 元曲需要按行解析
		// 元曲最后一行多了一个中括号, see https://github.com/chinese-poetry/chinese-poetry/issues/256
		if isYuanqu {
			f, _ := os.Open(jsonFile.filePath)
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				parseJsonByLine(scanner.Bytes(), jsonFile.category, jsonFile.dynasty)
			}
		} else {
			bytes, err := ioutil.ReadFile(jsonFile.filePath)
			if err != nil {
				log.Fatal(err)
			}

			_, err = jsonparser.ArrayEach(
				bytes, func(
					value []byte,
					dataType jsonparser.ValueType,
					offset int,
					err error) {

					parseJsonByLine(value, jsonFile.category, jsonFile.dynasty)

				})

			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

// 解析每一个poem json对象
func parseJsonByLine(bytes []byte, category string, dynasty string) {
	title := getStringField("title", bytes)
	author := getStringField("author", bytes)
	rhythmic := getStringField("rhythmic", bytes)
	chapter := getStringField("chapter", bytes)
	section := getStringField("section", bytes)
	notes := getStringArrayField("notes", bytes)
	paragraphs := getStringArrayField("paragraphs", bytes)

	if len(paragraphs) <= 0 {
		paragraphs = getStringArrayField("content", bytes)
	}

	if len(paragraphs) <= 0 {
		paragraphs = getStringArrayField("comment", bytes)
	}

	if len(paragraphs) <= 0 {
		return
	}

	if needFilter(title, author, rhythmic, chapter, section, notes, paragraphs) {
		return
	}

	fmt.Printf("(%d) title %s, rhythmic %s  \n", counter, title, rhythmic)

	counter++

	appendInsertSqlByLine(category, dynasty, title, author, rhythmic, chapter, section, notes, paragraphs)
}

// 简繁转换
func translation() {
	println("start translation")
	traditionalFile, _ := os.Open(sqlFileHantPath)
	scanner := bufio.NewScanner(traditionalFile)
	defer traditionalFile.Close()

	simplifiedFile, err := os.OpenFile(sqlFileHansPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	defer simplifiedFile.Close()
	if err != nil {
		panic(err)
	}

	cc, err := opencc.NewOpenCC("t2s", openccDataDir)
	if err != nil {
		fmt.Println(err)
	}

	for scanner.Scan() {
		text := scanner.Text()

		translation, err := cc.ConvertText(text)
		if err != nil {
			fmt.Println(err)
		}

		translation += "\n"

		if _, err = simplifiedFile.WriteString(translation); err != nil {
			panic(err)
		}

		if err != nil {
			log.Fatal(err)
		}
	}
}

// 过滤
func needFilter(
	title string,
	author string,
	rhythmic string,
	chapter string,
	section string,
	notes []string,
	paragraphs []string) bool {

	// title 或者 author 过长的就是错误数据
	if len(title) > 30 || len(author) > 10 {
		return true
	}

	// 过滤掉乱码内容
	for _, value := range []string{rhythmic, chapter, section, flatStringArray(notes), flatStringArray(paragraphs)} {
		if len(value) > 0 && strings.Contains(value, "□") {
			return true
		}
	}

	return false
}

func getStringArrayField(field string, bytes []byte) []string {
	var values []string
	_, err := jsonparser.ArrayEach(bytes, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		values = append(values, string(value))
	}, field)

	if err != nil {
		return []string{}
	}

	return values
}

func getStringField(filed string, bytes []byte) string {
	value, err := jsonparser.GetString(bytes, filed)

	if err != nil {
		return ""
	}

	return value
}

// 把数组转换成字符串,用|分割
func flatStringArray(array []string) string {
	return strings.Join(array, "|")
}

// 一行一行的写入insert sql
func appendInsertSqlByLine(
	category string,
	dynasty string,
	title string,
	author string,
	rhythmic string,
	chapter string,
	section string,
	notes []string,
	paragraphs []string) {

	values := fmt.Sprintf(
		"INSERT INTO `poems` (`category`,`dynasty`,`title`,`author`,`rhythmic`,`chapter`,`section`,`notes`,`paragraphs`) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s');\n",
		category,
		dynasty,
		escape(title),
		escape(author),
		escape(rhythmic),
		escape(chapter),
		escape(section),
		escape(flatStringArray(notes)),
		escape(flatStringArray(paragraphs)),
	)

	f, err := os.OpenFile(sqlFileHantPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	if _, err = f.WriteString(values); err != nil {
		panic(err)
	}

	if err != nil {
		log.Fatal(err)
	}
}

// 对字符串进行转义
func escape(value string) string {
	// <\"> => <">
	return strings.Replace(value, "\\\"", "\"", -1)
}

// 写入到数据库
func writeToDatabase() {
	db, err := sql.Open("sqlite3", databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	f, err := os.Open(sqlFileHansPath)
	if err != nil {
		log.Fatal(err)
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		sqlString := scanner.Text()
		_, err = db.Exec(sqlString)

		if err != nil {
			log.Printf("%q \n", err)
			return
		}
	}
}

// 将原始Json文件导入成JsonFile结构数组
func importToJsonFiles() []JsonFile {
	jsonFiles := make([]JsonFile, 0)
	jsonFiles = append(jsonFiles, createJsonFiles("json", "poet.tang.*.json", "shi", "tang")...)
	jsonFiles = append(jsonFiles, createJsonFiles("json", "poet.song.*.json", "shi", "song")...)
	jsonFiles = append(jsonFiles, createJsonFiles("ci", "ci.song.*.json", "ci", "song")...)
	jsonFiles = append(jsonFiles, createJsonFiles("yuanqu", "yuanqu.json", "qu", "yuan")...)
	jsonFiles = append(jsonFiles, createJsonFiles("shijing", "shijing.json", "shige", "zhou")...)
	return jsonFiles
}

func main() {
	cleanup()
	cloneRepo()
	createDatabaseAndTable()
	parseJson()
	translation()
	writeToDatabase()
}
