import os

from rdfc_runner.server.whitelist import build_whitelist


def write(tmp_path, name, content):
    path = tmp_path / name
    path.write_text(content)
    return str(path)


def test_whitelist_follows_file_imports(tmp_path):
    imported = write(tmp_path, "helper.ttl", "")
    root = write(tmp_path, "processors.ttl", """
        @prefix owl: <http://www.w3.org/2002/07/owl#>.
        <> owl:imports <./helper.ttl>.
    """)

    whitelist = build_whitelist([root])

    assert whitelist == {os.path.realpath(root), os.path.realpath(imported)}


def test_whitelist_terminates_on_cycles(tmp_path):
    a = tmp_path / "a.ttl"
    b = tmp_path / "b.ttl"
    a.write_text('@prefix owl: <http://www.w3.org/2002/07/owl#>. <> owl:imports <./b.ttl>.')
    b.write_text('@prefix owl: <http://www.w3.org/2002/07/owl#>. <> owl:imports <./a.ttl>.')

    whitelist = build_whitelist([str(a)])

    assert whitelist == {os.path.realpath(str(a)), os.path.realpath(str(b))}


def test_whitelist_keeps_missing_files_without_following(tmp_path):
    root = write(tmp_path, "processors.ttl", """
        @prefix owl: <http://www.w3.org/2002/07/owl#>.
        <> owl:imports <./missing.ttl>.
    """)

    whitelist = build_whitelist([root])

    assert os.path.realpath(root) in whitelist
    assert os.path.realpath(str(tmp_path / "missing.ttl")) in whitelist
    assert len(whitelist) == 2


def test_whitelist_ignores_http_imports_and_foreign_subjects(tmp_path):
    other = write(tmp_path, "other.ttl", "")
    root = write(tmp_path, "processors.ttl", f"""
        @prefix owl: <http://www.w3.org/2002/07/owl#>.
        <> owl:imports <http://example.org/remote.ttl>.
        <http://example.org/doc> owl:imports <./other.ttl>.
    """)

    whitelist = build_whitelist([root])

    # http import ignored; the file import is ignored too since its subject is not the document.
    assert whitelist == {os.path.realpath(root)}
    assert os.path.realpath(other) not in whitelist
