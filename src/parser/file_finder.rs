use crate::errors::{AppError, AppResult};
use std::path::PathBuf;

/// Finds all XML/Atom files organized by subdirectory.
///
/// This function scans the immediate subdirectories of the given path and
/// recursively collects all `.xml` and `.atom` files within each subdirectory.
/// Files in the top-level directory are ignored.
///
/// # Returns
///
/// Returns a vector of tuples where:
/// - First element: Subdirectory name (e.g., "202301")
/// - Second element: Vector of paths to XML/Atom files found in that subdirectory
///
/// Only subdirectories containing at least one XML/Atom file are included.
///
/// # Arguments
///
/// * `path` - Base directory to search (typically the extraction directory)
///
/// # Errors
///
/// Returns an error if directory reading fails.
pub fn find_xmls(path: &std::path::Path) -> AppResult<Vec<(String, Vec<PathBuf>)>> {
    // Pre-allocate with conservative estimate (usually 1-100 subdirectories)
    let mut out = Vec::with_capacity(50);

    for subdir in std::fs::read_dir(path).map_err(AppError::from)? {
        let subdir = subdir.map_err(AppError::from)?;
        let file_type = subdir.file_type().map_err(AppError::from)?;
        if file_type.is_dir() {
            let subdir_path = subdir.path();
            let files = collect_xmls(&subdir_path);
            if !files.is_empty() {
                let name = subdir_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("")
                    .to_string();
                out.push((name, files));
            }
        }
    }

    Ok(out)
}

/// Recursively collects `.xml` or `.atom` files in a directory (including subdirs).
pub(crate) fn collect_xmls(dir: &std::path::Path) -> Vec<PathBuf> {
    // Pre-allocate with conservative estimate (usually 1-20 XML files per directory)
    let mut v = Vec::with_capacity(20);
    let walker = walkdir::WalkDir::new(dir).into_iter();
    for entry in walker.flatten() {
        if entry.file_type().is_file() {
            if let Some(ext) = entry.path().extension().and_then(|e| e.to_str()) {
                if ext.eq_ignore_ascii_case("xml") || ext.eq_ignore_ascii_case("atom") {
                    v.push(entry.path().to_path_buf());
                }
            }
        }
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use tempfile::TempDir;

    // Helper function to create a test XML file
    fn create_test_xml_file(path: &std::path::Path, content: &str) {
        let parent = path.parent().unwrap();
        fs::create_dir_all(parent).unwrap();
        fs::File::create(path)
            .unwrap()
            .write_all(content.as_bytes())
            .unwrap();
    }

    #[test]
    fn test_collect_xmls_recursive() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().join("base");
        fs::create_dir_all(&base_dir).unwrap();

        // Create nested structure
        let subdir = base_dir.join("subdir");
        fs::create_dir_all(&subdir).unwrap();
        fs::create_dir_all(subdir.join("nested")).unwrap();

        // Create XML and ATOM files at different levels
        create_test_xml_file(&base_dir.join("file1.xml"), "<feed></feed>");
        create_test_xml_file(&subdir.join("file2.xml"), "<feed></feed>");
        create_test_xml_file(&subdir.join("nested/file3.atom"), "<feed></feed>");
        create_test_xml_file(&base_dir.join("file.txt"), "not xml");
        create_test_xml_file(&base_dir.join("file.XML"), "<feed></feed>");
        create_test_xml_file(&base_dir.join("file.ATOM"), "<feed></feed>");

        let files = collect_xmls(&base_dir);
        assert_eq!(files.len(), 5); // file1.xml, file2.xml, file3.atom, file.XML, file.ATOM
        assert!(files.iter().any(|p| p.ends_with("file1.xml")));
        assert!(files.iter().any(|p| p.ends_with("file2.xml")));
        assert!(files.iter().any(|p| p.ends_with("file3.atom")));
        assert!(files.iter().any(|p| p.ends_with("file.XML")));
        assert!(files.iter().any(|p| p.ends_with("file.ATOM")));
        assert!(!files.iter().any(|p| p.ends_with("file.txt")));
    }

    #[test]
    fn test_collect_xmls_case_insensitive() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().join("base");
        fs::create_dir_all(&base_dir).unwrap();

        create_test_xml_file(&base_dir.join("lower.xml"), "<feed></feed>");
        create_test_xml_file(&base_dir.join("UPPER.XML"), "<feed></feed>");
        create_test_xml_file(&base_dir.join("Mixed.Xml"), "<feed></feed>");
        create_test_xml_file(&base_dir.join("lower.atom"), "<feed></feed>");
        create_test_xml_file(&base_dir.join("UPPER.ATOM"), "<feed></feed>");
        create_test_xml_file(&base_dir.join("Mixed.Atom"), "<feed></feed>");

        let files = collect_xmls(&base_dir);
        assert_eq!(files.len(), 6);
    }

    #[test]
    fn test_find_xmls_with_subdirectories() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().join("extract");
        fs::create_dir_all(&base_dir).unwrap();

        // Create subdirectories
        let subdir1 = base_dir.join("202301");
        let subdir2 = base_dir.join("202302");
        fs::create_dir_all(&subdir1).unwrap();
        fs::create_dir_all(&subdir2).unwrap();

        // Add XML files to subdirectories
        create_test_xml_file(&subdir1.join("file1.xml"), "<feed></feed>");
        create_test_xml_file(&subdir1.join("file2.xml"), "<feed></feed>");
        create_test_xml_file(&subdir2.join("file1.atom"), "<feed></feed>");

        // Add non-XML file (should be ignored)
        create_test_xml_file(&subdir2.join("file.txt"), "text");

        // Add file at top level (should be ignored)
        create_test_xml_file(&base_dir.join("top.xml"), "<feed></feed>");

        let result = find_xmls(&base_dir).unwrap();
        assert_eq!(result.len(), 2);

        let (name1, files1) = result.iter().find(|(n, _)| n == "202301").unwrap();
        assert_eq!(name1, "202301");
        assert_eq!(files1.len(), 2);

        let (name2, files2) = result.iter().find(|(n, _)| n == "202302").unwrap();
        assert_eq!(name2, "202302");
        assert_eq!(files2.len(), 1);
    }

    #[test]
    fn test_find_xmls_empty_directories() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().join("extract");
        fs::create_dir_all(&base_dir).unwrap();

        // Create empty subdirectory
        fs::create_dir_all(base_dir.join("empty")).unwrap();

        // Create subdirectory with only non-XML files
        let no_xml_dir = base_dir.join("no_xml");
        fs::create_dir_all(&no_xml_dir).unwrap();
        create_test_xml_file(&no_xml_dir.join("file.txt"), "text");

        let result = find_xmls(&base_dir).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_find_xmls_nested_structure() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().join("extract");
        fs::create_dir_all(&base_dir).unwrap();

        let subdir = base_dir.join("202301");
        fs::create_dir_all(&subdir).unwrap();
        fs::create_dir_all(subdir.join("level1/level2")).unwrap();

        create_test_xml_file(&subdir.join("file1.xml"), "<feed></feed>");
        create_test_xml_file(&subdir.join("level1/file2.xml"), "<feed></feed>");
        create_test_xml_file(&subdir.join("level1/level2/file3.atom"), "<feed></feed>");

        let result = find_xmls(&base_dir).unwrap();
        assert_eq!(result.len(), 1);
        let (_, files) = &result[0];
        assert_eq!(files.len(), 3);
    }
}
