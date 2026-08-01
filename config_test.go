package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadEnvFile(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("基本解析", func(t *testing.T) {
		path := filepath.Join(tmpDir, ".env")
		os.WriteFile(path, []byte("KEY1=val1\nKEY2=val2\n"), 0644)

		if err := loadEnvFile(path); err != nil {
			t.Fatal(err)
		}
		if os.Getenv("KEY1") != "val1" {
			t.Errorf("KEY1 = %q", os.Getenv("KEY1"))
		}
		if os.Getenv("KEY2") != "val2" {
			t.Errorf("KEY2 = %q", os.Getenv("KEY2"))
		}
	})

	t.Run("跳过注释和空行", func(t *testing.T) {
		path := filepath.Join(tmpDir, "with_comments.env")
		os.WriteFile(path, []byte("# 这是注释\nKEY=val\n\n# 另一个注释\nKEY2=val2\n"), 0644)

		if err := loadEnvFile(path); err != nil {
			t.Fatal(err)
		}
		if os.Getenv("KEY") != "val" {
			t.Errorf("KEY = %q", os.Getenv("KEY"))
		}
	})

	t.Run("去除引号", func(t *testing.T) {
		path := filepath.Join(tmpDir, "quoted.env")
		os.WriteFile(path, []byte(`KEY="val with spaces"`+"\n"), 0644)

		if err := loadEnvFile(path); err != nil {
			t.Fatal(err)
		}
		if os.Getenv("KEY") != "val with spaces" {
			t.Errorf("KEY = %q", os.Getenv("KEY"))
		}
	})

	t.Run("文件不存在不报错", func(t *testing.T) {
		if err := loadEnvFile("/nonexistent/.env"); err != nil {
			t.Errorf("expected nil for missing file, got %v", err)
		}
	})

	t.Run("值中包含等号", func(t *testing.T) {
		path := filepath.Join(tmpDir, "eq.env")
		os.WriteFile(path, []byte("URL=http://example.com?a=1\n"), 0644)

		if err := loadEnvFile(path); err != nil {
			t.Fatal(err)
		}
		if os.Getenv("URL") != "http://example.com?a=1" {
			t.Errorf("URL = %q", os.Getenv("URL"))
		}
	})

	t.Run("空白去除", func(t *testing.T) {
		path := filepath.Join(tmpDir, "spaces.env")
		os.WriteFile(path, []byte("  KEY  =  val  \n"), 0644)

		if err := loadEnvFile(path); err != nil {
			t.Fatal(err)
		}
		if os.Getenv("KEY") != "val" {
			t.Errorf("KEY = %q, want %q", os.Getenv("KEY"), "val")
		}
	})
}
