import pytest, asyncio, aiohttp.web
from lanscatter import fileio, chunker
from pathlib import Path


def test_fileio(tmp_path):
    """
    Some important extra tests not covered by integration ones.
    """
    async def aiotests():
        fio = fileio.FileIO(Path(tmp_path))

        with pytest.raises(PermissionError):
            fio.resolve_and_sanitize('../abba123')

        with pytest.raises(PermissionError):
            fio.resolve_and_sanitize('/abba123')

        with pytest.raises(FileNotFoundError):
            fio.resolve_and_sanitize('abba123', must_exist=True)

        with pytest.raises(FileNotFoundError):
            fio.open_and_seek('abba123', 0, for_write=False)

        with pytest.raises(PermissionError):
            fio.open_and_seek('../abba123', 0, for_write=False)

        with pytest.raises(aiohttp.web.HTTPForbidden):
            await fio.upload_chunk(chunker.FileChunk(path='../abba1234', pos=0, size=123, cmpratio=1, hash='abcdef1234'), None)

        with pytest.raises(aiohttp.web.HTTPInternalServerError):
            await fio.upload_chunk(chunker.FileChunk(path=False, pos=0, size=123, cmpratio=1, hash='abcdef1234'), None)

        assert await fio.copy_chunk_locally(
                chunker.FileChunk(path='abba1234', pos=0, size=123, cmpratio=1, hash='abcdef1234'),
                chunker.FileChunk(path='abba1234', pos=0, size=123, cmpratio=1, hash='abcdef1234')) is True

        with pytest.raises(ValueError):
            assert await fio.copy_chunk_locally(
                    chunker.FileChunk(path='abba1234', pos=0, size=123, cmpratio=1, hash='abcdef1234'),
                    chunker.FileChunk(path='asdfklna', pos=0, size=123, cmpratio=1, hash='ABABABABAB'))

        assert fio.try_precreate_large_sparse_file('sparsefiletest.bin', 1234) == fio.resolve_and_sanitize('sparsefiletest.bin').exists()
        await fio.remove_file_and_paths('sparsefiletest.bin')

    asyncio.run(aiotests())